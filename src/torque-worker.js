'use strict';

/**
 * 带载安全选配专用计算线程（worker_threads）
 *
 * 完整速比枚举（enumerateMatches）与全部级次顺序的扭矩校核（planTorque）
 * 均为 CPU 密集型计算，统一放在本线程执行，主线程因此可以继续响应
 * 页面静态资源与其他 API。
 *
 * 协议（主线程 TorqueWorkerPool 保证同时只有一个在算请求）：
 *   收 { id, payload: { gears, target, tolerance, inputTorque } }
 *   回 { id, ok: true, plan }（plan 中 BigInt 分数经结构化克隆原样保留）
 *   或 { id, ok: false, error }（仅本请求失败，线程继续存活）
 * 响应按请求 id 一一对应，id 由主线程分配。
 */

const { parentPort } = require('worker_threads');
const { planTorque } = require('./torque');

parentPort.on('message', (msg) => {
  try {
    const { gears, target, tolerance, inputTorque } = msg.payload;
    const plan = planTorque(gears, target, tolerance, inputTorque);
    parentPort.postMessage({ id: msg.id, ok: true, plan });
  } catch (err) {
    // 捕获得到的计算异常只失败当前请求，线程不退出、继续处理后续请求
    parentPort.postMessage({
      id: msg.id,
      ok: false,
      error: err && err.stack ? err.stack : String(err),
    });
  }
});
