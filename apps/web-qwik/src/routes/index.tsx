import { component$, useSignal, useVisibleTask$, $ } from '@builder.io/qwik';
import { Centrifuge } from 'centrifuge';
import { createClient, ConnectError, Code, type Interceptor } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';
// Импортируем сгенерированный из platform.proto сервис
import { CatalogService } from '../gen/platform_pb';

export default component$(() => {
  const username = useSignal('');
  const password = useSignal('');
  const productId = useSignal('1001');
  const accessToken = useSignal<string | null>(null);
  const logs = useSignal<string>('Инициализация клиентского BFF контура завершена.');

  // 🌟 1. ПОДКЛЮЧЕНИЕ К REAL-TIME СЕРВЕРУ CENTRIFUGO ПО WEBSOCKET
  useVisibleTask$(({ cleanup }) => {
    const centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket');

    centrifuge.on('connected', () => {
      logs.value += '\n🟢 [WebSocket Centrifugo] Подключено к каналу live-updates:products';
    });

    const sub = centrifuge.newSubscription('live-updates:products');

    sub.on('publication', (ctx) => {
      logs.value += `\n⚡ [LIVE PUSH из БД]: Прилетел новый товар -> ${JSON.stringify(ctx.data)}`;
    });

    sub.subscribe();
    centrifuge.connect();

    cleanup(() => {
      centrifuge.disconnect();
    });
  });

  // 🌟 2. ФАБРИКА СТРОГО ТИПИЗИРОВАННОГО CONNECT-RPC КЛИЕНТА (BINARY PROTOBUF + SILENT REFRESH)
  const getCatalogClient = $(() => {
    const authAndRefreshInterceptor: Interceptor = (next) => async (req) => {
      if (accessToken.value) {
        req.header.set('Authorization', `Bearer ${accessToken.value}`);
      }

      try {
        return await next(req);
      } catch (err) {
        // Если KrakenD отклонил запрос из-за истекшего Access-токена (Unauthenticated / 401)
        if (err instanceof ConnectError && err.code === Code.Unauthenticated) {
          logs.value += '\n[BFF] Access Токен истек. Выполняю тихий перезапрос через Refresh куку...';

          const refreshRes = await fetch('http://localhost:8080/api/auth/refresh', {
            method: 'POST',
            credentials: 'include',
          });

          if (refreshRes.ok) {
            const data = await refreshRes.json();
            accessToken.value = data.access_token;
            logs.value += '\n[BFF] Токен успешно обновлен. Повторяю бинарный RPC-вызов...';

            req.header.set('Authorization', `Bearer ${accessToken.value}`);
            return await next(req);
          } else {
            logs.value += '\n[Ошибка] Refresh-сессия устарела. Требуется полный перезаход.';
            accessToken.value = null;
          }
        }
        throw err;
      }
    };

    const transport = createConnectTransport({
      // Маршрут /rpc в KrakenD проксирует бинарный трафик в core-go:8082
      baseUrl: 'http://localhost:8080/rpc',
      useBinaryFormat: true,
      interceptors: [authAndRefreshInterceptor],
    });

    return createClient(CatalogService, transport);
  });

  // --- REST-МЕТОДЫ АУТЕНТИФИКАЦИИ (ДЛЯ РАБОТЫ С HTTPONLY COOKIES) ---

  const onRegister = $(async () => {
    const res = await fetch('http://localhost:8080/api/auth/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: username.value, password: password.value }),
    });
    logs.value += res.ok
      ? `\n[БД Yugabyte] Зарегистрирован пользователь: ${username.value}`
      : `\n[Ошибка регистрации]: ${res.status} ${res.statusText}`;
  });

  const onLogin = $(async () => {
    const res = await fetch('http://localhost:8080/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ username: username.value, password: password.value }),
    });
    if (res.ok) {
      const data = await res.json();
      accessToken.value = data.access_token;
      logs.value += '\n[Вход] Сессия создана. Access-токен в памяти, Refresh в HttpOnly Cookie.';
    } else {
      logs.value += '\n[Вход] Ошибка аутентификации (неверный логин или пароль).';
    }
  });

  const onLogout = $(async () => {
    await fetch('http://localhost:8080/api/auth/logout', {
      method: 'POST',
      credentials: 'include',
    });
    accessToken.value = null;
    logs.value += '\n[Выход] Токены аннулированы в Dragonfly, куки стерты.';
  });

  // --- БИНАРНЫЕ МЕТОДЫ CONNECT-RPC ДЛЯ КАТАЛОГА (CQRS) ---

  const onGetProduct = $(async () => {
    try {
      const client = await getCatalogClient();
      const response = await client.getProduct({ productId: productId.value });
      logs.value += `\n[Connect-RPC Чтение | Источник: ${response.source}]: ID=${response.productId}, Title="${response.title}", Attrs=${response.jsonbAttributes}`;
    } catch (err) {
      const msg = err instanceof ConnectError ? `${err.code}: ${err.message}` : String(err);
      logs.value += `\n[KrakenD / Connect-RPC Защита] Доступ заблокирован: ${msg}`;
    }
  });

  const onAddProduct = $(async () => {
    try {
      const client = await getCatalogClient();
      const response = await client.addProduct({
        productId: productId.value,
        title: `Товар #${productId.value} из YugabyteDB`,
        jsonbAttributes: '{"color": "black", "size": "42", "verified": true}',
      });
      logs.value += `\n[Connect-RPC Запись CQRS]: status=${response.status}, product_id=${response.productId}`;
    } catch (err) {
      const msg = err instanceof ConnectError ? `${err.code}: ${err.message}` : String(err);
      logs.value += `\n[Connect-RPC Запись Ошибка]: ${msg}`;
    }
  });

  return (
    <div style={{ padding: '30px', fontFamily: 'monospace', background: '#f5f5f5', minHeight: '100vh' }}>
      <h2>Панель Монорепозитория Enterprise уровня (2026)</h2>

      <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 2px 4px rgba(0,0,0,0.1)' }}>
        <h3>1. Аутентификация (Zero-Trust Session)</h3>
        <input type="text" placeholder="Username" bind:value={username} /> &nbsp;
        <input type="password" placeholder="Password" bind:value={password} />
        <br />
        <br />
        <button onClick$={onRegister}>Регистрация</button> &nbsp;
        <button onClick$={onLogin} style={{ background: '#4CAF50', color: 'white', border: 'none', padding: '6px 12px', cursor: 'pointer' }}>
          Войти
        </button>{' '}
        &nbsp;
        <button onClick$={onLogout} style={{ background: '#f44336', color: 'white', border: 'none', padding: '6px 12px', cursor: 'pointer' }}>
          Выйти
        </button>
      </div>

      <br />

      <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 2px 4px rgba(0,0,0,0.1)' }}>
        <h3>2. Каталог Товаров (Binary Connect-RPC + CQRS + WebSocket)</h3>
        <input type="text" placeholder="Product ID" bind:value={productId} /> &nbsp;
        <button onClick$={onGetProduct} style={{ padding: '6px 12px', cursor: 'pointer' }}>
          Получить товар (RPC)
        </button>{' '}
        &nbsp;
        <button onClick$={onAddProduct} style={{ background: '#2196F3', color: 'white', border: 'none', padding: '6px 12px', cursor: 'pointer' }}>
          Добавить товар (RPC)
        </button>
      </div>

      <h3>Логи транзакций, кэша Dragonfly и Live-событий Centrifugo:</h3>
      <pre style={{ background: '#1e1e1e', color: '#00ff00', padding: '20px', borderRadius: '6px', whiteSpace: 'pre-wrap', lineHeight: '1.5' }}>
        {logs}
      </pre>
    </div>
  );
});
