import { component$, useSignal, $ } from '@builder.io/qwik';

export default component$(() => {
  const username = useSignal('');
  const password = useSignal('');
  const productId = useSignal('1001');
  const accessToken = useSignal<string | null>(null);
  const logs = useSignal<string>('Инициализация клиентского BFF контура завершена.');

  // 🌟 Перехватчик: Безопасная типизация заголовков и включение credentials для отправки кук
  const fetchWithInterceptor = $(async (url: string, init: RequestInit = {}): Promise<Response> => {
    const headers = new Headers(init.headers);
    if (accessToken.value) {
      headers.set('Authorization', `Bearer ${accessToken.value}`);
    }
    init.headers = headers;
    init.credentials = 'include'; // 🌟 ВАЖНО: Разрешает браузеру прикреплять HttpOnly Refresh-куку

    let res = await fetch(url, init);

    if (res.status === 401) {
      logs.value += '\n[BFF] Access Токен истек. Выполняю тихий перезапрос через Refresh куку...';
      
      const refreshRes = await fetch('http://localhost:8080/api/auth/refresh', { 
        method: 'POST',
        credentials: 'include' // 🌟 Отправляем куку на роут обновления сессии
      });

      if (refreshRes.ok) {
        const data = await refreshRes.json();
        accessToken.value = data.access_token;
        logs.value += '\n[BFF] Токен успешно обновлен. Повторяю операцию...';
        
        const retryHeaders = new Headers(init.headers);
        retryHeaders.set('Authorization', `Bearer ${accessToken.value}`);
        init.headers = retryHeaders;
        
        res = await fetch(url, init);
      } else {
        logs.value += '\n[Ошибка] Refresh-сессия устарела. Требуется полный перезаход.';
        accessToken.value = null;
      }
    }
    return res;
  });

  const onRegister = $(async () => {
    const res = await fetch('http://localhost:8080/api/auth/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: username.value, password: password.value })
    });
    logs.value += res.ok ? `\n[БД Yugabyte] Зарегистрирован пользователь: ${username.value}` : `\n[Ошибка]: ${res.statusText}`;
  });

  const onLogin = $(async () => {
    const res = await fetch('http://localhost:8080/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: username.value, password: password.value })
    });
    if (res.ok) {
      const data = await res.json();
      accessToken.value = data.access_token;
      logs.value += '\n[Вход] Сессия создана. Токен сохранен в памяти.';
    } else {
      logs.value += '\n[Вход] Ошибка аутентификации.';
    }
  });

  const onLogout = $(async () => {
    await fetch('http://localhost:8080/api/auth/logout', { 
      method: 'POST',
      credentials: 'include' 
    });
    accessToken.value = null;
    logs.value += '\n[Выход] Токены аннулированы в Dragonfly, куки стерты.';
  });

  const onGetProduct = $(async () => {
    const res = await fetchWithInterceptor(`http://localhost:8080/api/catalog/product/${productId.value}`);
    if (res.ok) {
      const data = await res.json();
      logs.value += `\n[gRPC Python -> Go]: ${JSON.stringify(data)}`;
    } else {
      logs.value += `\n[KrakenD Защита] Доступ заблокирован шлюзом: Код ${res.status}`;
    }
  });

  const onAddProduct = $(async () => {
    const res = await fetchWithInterceptor('http://localhost:8080/api/catalog/product', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 
        product_id: productId.value, 
        title: 'Реальный Товар из YugabyteDB', 
        jsonb_attributes: '{"color": "black", "size": "42"}' 
      })
    });
    if (res.ok) {
      const data = await res.json();
      logs.value += `\n[Запись CQRS]: ${JSON.stringify(data)}`;
    } else {
      logs.value += `\n[Запись CQRS Ошибка]: Код ${res.status}`;
    }
  });

  return (
    <div style={{ padding: '30px', fontFamily: 'monospace', background: '#f5f5f5', minHeight: '100vh' }}>
      <h2>Панель Монорепозитория Enterprise уровня</h2>
      
      <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 2px 4px rgba(0,0,0,0.1)' }}>
        <h3>Пользовательские данные</h3>
        {/* 🌟 ИСПРАВЛЕНО: передаем сигналы целиком для bind:value */}
        <input type="text" placeholder="Username" bind:value={username} /> &nbsp;
        <input type="password" placeholder="Password" bind:value={password} />
        <br/><br/>
        <button onClick$={onRegister}>Регистрация</button> &nbsp;
        <button onClick$={onLogin} style={{ background: '#4CAF50', color: 'white' }}>Войти</button> &nbsp;
        <button onClick$={onLogout} style={{ background: '#f44336', color: 'white' }}>Выйти</button>
      </div>
      
      <br/>
      
      <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 2px 4px rgba(0,0,0,0.1)' }}>
        <h3>Проверка Каталога и CQRS записи</h3>
        {/* 🌟 ИСПРАВЛЕНО: передаем сигнал целиком */}
        <input type="text" placeholder="Product ID" bind:value={productId} /> &nbsp;
        <button onClick$={onGetProduct}>Получить товар по gRPC</button> &nbsp;
        <button onClick$={onAddProduct} style={{ background: '#2196F3', color: 'white' }}>Добавить товар (CQRS POST)</button>
      </div>

      <h3>Логи транзакций и безопасности:</h3>
      {/* 🌟 ИСПРАВЛЕНО: Выводим объект логов без .value для гранулярного обновления DOM */}
      <pre style={{ background: '#222', color: '#00ff00', padding: '20px', borderRadius: '6px', whiteSpace: 'pre-wrap' }}>{logs}</pre>
    </div>
  );
});
