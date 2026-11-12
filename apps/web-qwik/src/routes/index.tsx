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
  const centrifugoToken = useSignal<string | null>(null);
  const logs = useSignal<string>('Инициализация клиентского BFF контура завершена.');

  // 🌟 1. АВТОРИЗОВАННОЕ ПОДКЛЮЧЕНИЕ К CENTRIFUGO ПО SSE (С ФОЛБЭКОМ НА WEBSOCKET)
  useVisibleTask$(({ track, cleanup }) => {
    // Отслеживаем только факт наличия сессии (boolean), чтобы обновление токена не пересоздавало соединение
    const isAuthenticated = track(() => Boolean(centrifugoToken.value));

    if (!isAuthenticated || !centrifugoToken.value) {
      return;
    }

    const centrifuge = new Centrifuge(
      [
        {
          transport: 'sse',
          endpoint: 'http://localhost:8000/connection/sse',
          // Обязательный эндпоинт Centrifugo v5 для передачи команд подписки в одностороннем SSE
          emulationEndpoint: 'http://localhost:8000/emulation',
        },
        {
          transport: 'websocket',
          endpoint: 'ws://localhost:8000/connection/websocket',
        },
      ],
      {
        token: centrifugoToken.value,
        getToken: async () => {
          const refreshRes = await fetch('http://localhost:8080/api/auth/refresh', {
            method: 'POST',
            credentials: 'include',
          });
          if (!refreshRes.ok) {
            throw new Error('Не удалось обновить токен для Centrifugo');
          }
          const data = await refreshRes.json();
          accessToken.value = data.access_token;
          centrifugoToken.value = data.centrifugo_token;
          return data.centrifugo_token;
        },
      }
    );

    centrifuge.on('connected', (ctx) => {
      // ctx.transport покажет 'sse' или 'websocket'
      logs.value += `\n🟢 [Centrifugo | Транспорт: ${ctx.transport.toUpperCase()}] Авторизовано (Client ID: ${ctx.client}) и подключено к live-updates:products`;
    });

    centrifuge.on('disconnected', (ctx) => {
      logs.value += `\n⚪ [Centrifugo] Соединение закрыто (${ctx.reason})`;
    });

    centrifuge.on('error', (ctx) => {
      logs.value += `\n🔴 [Centrifugo Ошибка]: ${JSON.stringify(ctx)}`;
    });

    const sub = centrifuge.newSubscription('live-updates:products');

    sub.on('publication', (ctx) => {
      logs.value += `\n⚡ [LIVE PUSH по SSE из Transactional Outbox]: ${JSON.stringify(ctx.data)}`;
    });

    sub.on('error', (ctx) => {
      logs.value += `\n🔴 [Ошибка подписки на канал]: ${JSON.stringify(ctx)}`;
    });

    sub.subscribe();
    centrifuge.connect();

    // Обязательная очистка соединения при выходе из аккаунта или закрытии вкладки
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
        // Если KrakenD или core-go отклонили запрос из-за истекшего Access-токена
        if (err instanceof ConnectError && err.code === Code.Unauthenticated) {
          logs.value += '\n[BFF] Access Токен истек. Выполняю тихий перезапрос через HttpOnly Refresh куку...';

          const refreshRes = await fetch('http://localhost:8080/api/auth/refresh', {
            method: 'POST',
            credentials: 'include',
          });

          if (refreshRes.ok) {
            const data = await refreshRes.json();
            accessToken.value = data.access_token;
            if (data.centrifugo_token) {
              centrifugoToken.value = data.centrifugo_token;
            }
            logs.value += '\n[BFF] Токены успешно обновлены. Повторяю бинарный RPC-вызов...';

            req.header.set('Authorization', `Bearer ${accessToken.value}`);
            return await next(req);
          } else {
            logs.value += '\n[Ошибка] Refresh-сессия устарела. Требуется полный перезаход.';
            accessToken.value = null;
            centrifugoToken.value = null;
          }
        }
        throw err;
      }
    };

    const transport = createConnectTransport({
      // Маршрут /rpc в KrakenD проксирует бинарный трафик в BFF-шлюз core-go:8082
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
      centrifugoToken.value = data.centrifugo_token;
      logs.value += '\n[Вход] Сессия создана. Access JWT в памяти, Refresh в HttpOnly Cookie, WS-токен передан в Centrifugo.';
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
    centrifugoToken.value = null;
    logs.value += '\n[Выход] Токены аннулированы в Dragonfly, куки стерты, сокет закрыт.';
  });

  // --- БИНАРНЫЕ МЕТОДЫ CONNECT-RPC ДЛЯ КАТАЛОГА (BFF core-go -> изолированный data-python) ---

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
        <h3>1. Аутентификация (Zero-Trust Session + Authorized WebSocket)</h3>
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
        <h3>2. Каталог Товаров (Binary Connect-RPC → BFF Go → Isolated Python → Outbox)</h3>
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
