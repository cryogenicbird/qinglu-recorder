# 录屏工作台（Qinglu）

带实时 AI 字幕的浏览器录屏工具：多素材 Canvas 合成录制 + 豆包大模型语音识别实时字幕。适合录制游戏实况、网课、演示视频。
成果视频：https://www.bilibili.com/video/BV1ub8Q6gEjk/
## 功能

- **多场景多素材**：显示器、摄像头、字幕三种素材，支持拖动、缩放、右键菜单（置顶/铺满/裁剪/移除）
- **Canvas 合成录制**：隐形画布合成多路画面 + 混合系统/麦克风音频，MediaRecorder 输出 webm
- **实时 AI 字幕**：麦克风音频经 WebSocket 推流至豆包 ASR，识别结果实时渲染进录制画面
- **稳定性设计**：断线自动重连（3 秒）、心跳保活（280 秒）、会话轮换（600 秒，防长会话识别降速）
- **全屏游戏录制不掉帧**：WebAudio ScriptProcessor 帧时钟，页面进入后台仍持续渲染
- **布局持久化**：localStorage 保存场景与素材布局，重启后以占位符恢复
- **录制帧率可选**：约 23 / 47 / 94 fps（受音频时钟量化，标签显示实际值）

## 架构

```
浏览器 (index.html)
  ├─ Canvas 合成器 ──captureStream──> MediaRecorder ──> 录制.webm
  ├─ 麦克风 PCM 16kHz ──WebSocket──> ws_server.py ──WebSocket──> 豆包 ASR（流式识别）
  └─ Flask (app.py) 仅提供静态页面 http://127.0.0.1:5000
```

字幕数据流：浏览器采集麦克风 → 分帧转 Int16 → base64 经 WebSocket 推送 → Python 中继按 Seed 协议打包转发豆包 → 识别文本回传 → 渲染到 Canvas 合成画面。

## 目录结构

```
index.html       前端单文件应用（CSS/JS 内联，无构建步骤）
app.py           Flask 静态服务（端口 5000）
ws_server.py     WebSocket 中继 + 豆包 ASR 会话管理（端口 5001）
start.bat        一键启动两个服务并打开页面
requirements.txt Python 依赖
recordings/      服务端保存的音频副本（已 gitignore）
```

## 快速开始

```bash
pip install -r requirements.txt
```

双击 `start.bat`，或手动运行：

```bash
py app.py         # 窗口 1
py ws_server.py   # 窗口 2
```

打开 http://127.0.0.1:5000。使用字幕功能需先在界面配置：添加字幕素材 → 右键 → 设置 API Key（火山引擎控制台的 APP ID / Access Token）。

## 技术要点

- **两层坐标系**：输出画布分辨率在录制开始时固定（保证视频参数稳定），素材节点位置每帧动态读取，通过 scaleX/scaleY 映射——窗口缩放、节点拖动互不干扰
- **音频帧时钟**：-60dB 静音振荡器保持 AudioContext 活跃 + ScriptProcessorNode 回调做帧时钟，利用"浏览器不降频发声页面"的机制，解决独占全屏下 rAF 停摆问题
- **快速失败传播**：服务端用 `asyncio.wait(FIRST_COMPLETED)` 竞速监测豆包会话，死亡即刻关闭浏览器连接，把异常秒级传播给前端触发重连
- **重连防重入**：手动停止标志位区分主动/被动断开；定时器去重 + 状态双重检查防重连风暴
- **占位符持久化**：屏幕采集必须用户手势触发，刷新后以占位符恢复布局，重新授权时原位替换

## 注意

- API 凭证仅通过界面填写，存于浏览器 localStorage，**不硬编码、不入库**
- 本项目为学习用途，未包含构建工具与打包配置
