"""业务路由汇总：恢复专用路由先于通用资源路由注册，避免动态路径抢匹配。"""
from __future__ import annotations

from app.routers import recovery, resource

ROUTERS = [recovery, resource]
