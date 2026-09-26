'use strict';

/**
 * /api/torque 计算线程池：单个 worker_threads + FIFO 等待队列
 *
 * - 完整速比枚举与级序载荷校核在独立线程执行，主线程始终可响应其他请求；
 * - 计算请求按到达顺序串行执行（单线程同一时刻只处理一个）；
 * - 最多允许 maxWaiting（默认 2）个等待请求；正在执行 1 个且队列已满时，
 *   新请求立即以 TORQUE_BUSY（HTTP 503）失败，不入队；
 * - 计算线程异常退出（未捕获异常 / 崩溃）时仅让当前请求失败，
 *   随后重建线程并继续处理队列中的等待请求；
 * - 每个请求带唯一 id，响应按 id 投递，并发请求结果不会串用。
 */

const path = require('path');
const { Worker } = require('worker_threads');

const DEFAULT_WORKER_FILE = path.join(__dirname, 'torque-worker.js');
/** 等待队列容量：1 个执行中 + 最多 2 个等待，超出立即 503 */
const DEFAULT_MAX_WAITING = 2;
/** 线程异常退出后的重建间隔（毫秒），避免崩溃循环时忙等 */
const RESPAWN_DELAY_MS = 100;

class TorquePoolError extends Error {
  /**
   * @param {string} code TORQUE_BUSY（队列满，503）/ TORQUE_WORKER_ERROR（500）
   * @param {string} message
   * @param {boolean} [retryable]
   */
  constructor(code, message, retryable = false) {
    super(message);
    this.name = 'TorquePoolError';
    this.code = code;
    this.retryable = retryable;
  }
}

class TorqueWorkerPool {
  /**
   * @param {{workerFile?:string,maxWaiting?:number}} [opts]
   *        workerFile 默认取 TORQUE_WORKER_FILE 环境变量或内置 torque-worker.js
   */
  constructor(opts = {}) {
    this.workerFile = opts.workerFile || process.env.TORQUE_WORKER_FILE || DEFAULT_WORKER_FILE;
    this.maxWaiting = opts.maxWaiting ?? DEFAULT_MAX_WAITING;
    /** @type {Array<{id:number,payload:object,resolve:Function,reject:Function}>} */
    this.queue = [];
    /** @type {{id:number,payload:object,resolve:Function,reject:Function}|null} */
    this.current = null;
    this.nextId = 1;
    this.worker = null;
    this.ready = false;
    this.closed = false;
    /** 崩溃后重建的定时器，便于测试时关闭 */
    this.respawnTimer = null;

    this._spawn();
  }

  /** 当前占用情况（用于 503 响应与观测） */
  stats() {
    return {
      running: this.current ? 1 : 0,
      waiting: this.queue.length,
      maxWaiting: this.maxWaiting,
    };
  }

  /**
   * 提交一个带载选配计算请求。
   * @param {object} payload 已通过主线程输入校验的计算参数
   * @returns {Promise<object>} worker 计算结果（plan）
   * @throws {TorquePoolError} 队列满（TORQUE_BUSY）或池已关闭
   */
  submit(payload) {
    if (this.closed) {
      throw new TorquePoolError('TORQUE_WORKER_ERROR', '带载选配计算服务正在启动，请稍后重试', true);
    }
    // 容量 = 1 个执行中 + maxWaiting 个等待；线程尚未就绪时队列中的请求同样计入
    const running = this.current ? 1 : 0;
    const waiting = this.queue.length;
    if (running + waiting >= 1 + this.maxWaiting) {
      throw new TorquePoolError(
        'TORQUE_BUSY',
        `带载选配计算线程繁忙：当前 ${running} 个计算进行中、${waiting} 个请求排队等待，` +
          `已达到容量上限（1 个执行中 + 最多 ${this.maxWaiting} 个等待）。请稍后重试。`,
        true,
      );
    }
    return new Promise((resolve, reject) => {
      this.queue.push({ id: this.nextId++, payload, resolve, reject });
      this._pump();
    });
  }

  /** 关闭池（进程退出 / 测试用）：拒绝全部未完成请求并终止线程 */
  close() {
    this.closed = true;
    if (this.respawnTimer) {
      clearTimeout(this.respawnTimer);
      this.respawnTimer = null;
    }
    const failAll = (job) => {
      if (job) {
        job.reject(new TorquePoolError('TORQUE_WORKER_ERROR', '带载选配计算服务正在关闭', false));
      }
    };
    failAll(this.current);
    this.current = null;
    while (this.queue.length) failAll(this.queue.shift());
    if (this.worker) {
      const w = this.worker;
      this.worker = null;
      w.terminate();
    }
  }

  /* ---------------------------------------------------------------- */

  /** 创建并注册新的计算线程 */
  _spawn() {
    this.ready = false;
    const worker = new Worker(this.workerFile);
    this.worker = worker;
    // 崩溃原因绑定到当前线程实例，避免上一个线程的错误信息串到下一个线程
    let crashError = null;

    worker.on('online', () => {
      this.ready = true;
      this._pump();
    });

    worker.on('message', (msg) => {
      // 只认当前在算请求的 id；任何不匹配的响应一律忽略，
      // 保证并发请求的结果绝不串用
      if (!this.current || msg.id !== this.current.id) return;
      const job = this.current;
      this.current = null;
      if (msg.ok) {
        job.resolve(msg.plan);
      } else {
        job.reject(new TorquePoolError('TORQUE_WORKER_ERROR', msg.error || '计算线程执行失败', false));
      }
      this._pump();
    });

    // error 事件（未捕获异常等）记录原因；Node 随后必然触发 exit，
    // 重建与当前请求失败统一在 exit 中处理一次
    worker.on('error', (err) => {
      crashError = err;
    });

    worker.on('exit', (code) => {
      if (this.worker !== worker) return; // 旧线程（close / 重建竞态）的迟到事件
      const err =
        crashError ||
        (code !== 0 ? new Error(`计算线程异常退出（exit code ${code}）`) : null);
      this._handleExit(err);
    });
  }

  /**
   * 线程退出处理：当前请求失败（仅限当前一个），重建线程后继续队列。
   * @param {Error|null} err null 表示线程正常退出（不应发生，同样重建）
   */
  _handleExit(err) {
    this.ready = false;
    this.worker = null;

    if (this.current) {
      const job = this.current;
      this.current = null;
      job.reject(
        new TorquePoolError(
          'TORQUE_WORKER_ERROR',
          err
            ? `带载选配计算线程异常退出，本次请求失败（已自动重建线程）：${err.message}`
            : '带载选配计算线程意外退出，本次请求失败（已自动重建线程）',
          false,
        ),
      );
    }

    if (this.closed) return;
    this.respawnTimer = setTimeout(() => {
      this.respawnTimer = null;
      if (this.closed) return;
      this._spawn();
    }, RESPAWN_DELAY_MS);
  }

  /** 把队首请求派发给线程（线程就绪且空闲时） */
  _pump() {
    if (this.current !== null || !this.ready || !this.worker || this.queue.length === 0) return;
    const job = this.queue.shift();
    this.current = job;
    this.worker.postMessage({ id: job.id, payload: job.payload });
  }
}

module.exports = {
  TorqueWorkerPool,
  TorquePoolError,
  DEFAULT_MAX_WAITING,
};
