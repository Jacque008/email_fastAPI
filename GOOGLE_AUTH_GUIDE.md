# Google 邮箱登录验证功能使用指南

## 概述

已为 FastAPI 应用添加了完整的 Google OAuth 2.0 登录验证功能，支持：

- Google 账号登录
- JWT Token 认证
- Session 会话管理
- 受保护的 API 端点
- 用户信息管理

## 设置步骤

### 1. Google Cloud Console 配置

1. 访问 [Google Cloud Console](https://console.cloud.google.com/)
2. 创建新项目或选择现有项目
3. 启用 Google+ API 和 Google OAuth2 API
4. 创建 OAuth 2.0 客户端 ID：
   - 应用类型：Web 应用
   - 授权重定向 URI：`http://localhost:8000/auth/google/callback`
   - 获取客户端 ID 和客户端密钥

### 2. 环境变量配置

在 `.env` 文件中设置以下变量：

```env
# Google OAuth 配置
GOOGLE_CLIENT_ID=你的客户端ID.apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=你的客户端密钥
GOOGLE_REDIRECT_URI=http://localhost:8000/auth/google/callback

# 安全密钥（生产环境请使用强密码）
SECRET_KEY=你的会话密钥
JWT_SECRET=你的JWT密钥
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 启动应用

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API 端点

### 认证相关

| 端点 | 方法 | 描述 | 认证要求 |
|------|------|------|----------|
| `/` | GET | 根路径 | 无 |
| `/health` | GET | 健康检查 | 无 |
| `/login` | GET | 登录页面 | 无 |
| `/login/google` | GET | Google 登录重定向 | 无 |
| `/auth/google/callback` | GET | OAuth 回调处理 | 无 |
| `/logout` | POST | 登出 | 需要 |
| `/me` | GET | 获取当前用户信息 | 需要 |

### 邮件处理

| 端点 | 方法 | 描述 | 认证要求 |
|------|------|------|----------|
| `/emails/example` | GET | 获取邮件示例 | 无 |
| `/emails/process` | POST | 处理邮件分类 | 需要 |

### 管理功能

| 端点 | 方法 | 描述 | 认证要求 |
|------|------|------|----------|
| `/admin/stats` | GET | 获取统计信息 | 需要 |

## 使用方式

### 1. 网页登录流程

1. 访问 `http://localhost:8000/login`
2. 点击 "Sign in with Google"
3. 在 Google 页面完成认证
4. 自动重定向回应用并获得 JWT token

### 2. API 调用认证

#### 方式一：使用 JWT Token

```bash
# 登录后获取 token
curl -X GET "http://localhost:8000/me" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

#### 方式二：使用 Session（浏览器）

```javascript
// 前端 JavaScript
fetch('/me', {
  method: 'GET',
  credentials: 'include'  // 包含 session cookies
})
```

### 3. 邮件处理示例

```bash
# 处理邮件（需要认证）
curl -X POST "http://localhost:8000/emails/process" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "subject": "Meeting Request",
      "sender": "john@example.com",
      "recipient": "jane@example.com", 
      "body": "Let'\''s meet tomorrow",
      "timestamp": "2024-01-15T10:30:00Z"
    }
  ]'
```

## 安全特性

1. **双重认证支持**：JWT Token 和 Session 并行支持
2. **邮箱验证**：只允许已验证的 Google 邮箱登录
3. **Token 过期**：JWT Token 24小时自动过期
4. **安全会话**：使用加密的 session middleware
5. **错误处理**：完整的错误处理和响应

## 前端集成示例

### HTML 登录页面

```html
<!DOCTYPE html>
<html>
<head>
    <title>Login</title>
</head>
<body>
    <div id="login-status"></div>
    <button onclick="login()">Login with Google</button>
    <button onclick="logout()">Logout</button>
    <button onclick="getProfile()">Get Profile</button>
    
    <script>
    async function login() {
        window.location.href = '/login/google';
    }
    
    async function logout() {
        const response = await fetch('/logout', {
            method: 'POST',
            credentials: 'include'
        });
        const data = await response.json();
        document.getElementById('login-status').innerHTML = data.message;
    }
    
    async function getProfile() {
        const response = await fetch('/me', {
            credentials: 'include'
        });
        const data = await response.json();
        document.getElementById('login-status').innerHTML = 
            `Hello, ${data.user.name} (${data.user.email})`;
    }
    </script>
</body>
</html>
```

## 自定义配置

### 修改 JWT 过期时间

在 `main.py` 中修改：

```python
JWT_EXPIRATION_HOURS = 24  # 改为所需的小时数
```

### 添加角色权限

可以扩展用户数据结构：

```python
user_data = {
    "sub": sub,
    "email": email,
    "name": name,
    "picture": picture,
    "provider": "google",
    "role": "user",  # 添加角色
    "permissions": ["read", "write"],  # 添加权限
    "authenticated_at": datetime.utcnow().isoformat()
}
```

## 故障排除

### 常见问题

1. **重定向 URI 不匹配**
   - 确保 Google Console 中的重定向 URI 与 `.env` 中的 `GOOGLE_REDIRECT_URI` 一致

2. **Token 验证失败**
   - 检查 `JWT_SECRET` 是否正确设置
   - 确认 token 未过期

3. **Session 丢失**
   - 检查 `SECRET_KEY` 配置
   - 确认前端请求包含 `credentials: 'include'`

### 日志调试

启动时添加调试日志：

```bash
uvicorn app.main:app --reload --log-level debug
```

## 生产环境部署注意事项

1. **使用强密码**：`SECRET_KEY` 和 `JWT_SECRET` 使用高强度随机密码
2. **HTTPS**：生产环境必须使用 HTTPS
3. **域名配置**：更新 Google Console 中的重定向 URI 为实际域名
4. **环境变量**：不要将敏感信息提交到代码仓库

这个实现提供了完整的 Google OAuth 认证功能，支持现代 Web 应用的认证需求。
