const { Telegraf } = require('telegraf');
const express = require('express');
const { Pool } = require('pg');
const Redis = require('ioredis');
const Queue = require('bull');
const winston = require('winston');
require('dotenv').config();

const app = express();
app.use(express.json());

// Конфигурация
const CONFIG = {
  TOKEN: process.env.TELEGRAM_TOKEN,
  POSTGRES_URL: process.env.POSTGRES_URL,
  REDIS_URL: process.env.REDIS_URL,
  WEBHOOK_URL: process.env.WEBHOOK_URL,
  ADMINS: [417951708],
  OFFER_DOC_URL: 'https://alfabank.servicecdn.ru/site-upload/64/1d/11483/Alfa_Future_Scholarships_For_Students_Regulations.pdf',
  APPLICATION_URL: 'https://docs.google.com/document/d/1XLokL8-yUEtJBZvm1ee1n59ScAmChoCD2JSa7-FP9LQ/edit?tab=t.0',
  SOPD_URL: 'https://docs.google.com/document/d/1DctmqN3xaEsbPk0yoIDsYTlbYoUOQyYIQCILdjZz0nI/edit?tab=t.0',
  POLICY_URL: 'https://alfabank.servicecdn.ru/site-upload/cb/dc/4263/pdn.pdf',
  FORM_URL: 'https://a28861.webask.io/ffbndoiha',
};

// Логирование
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'bot.log' })
  ]
});

// Инициализация Telegram-бота
const bot = new Telegraf(CONFIG.TOKEN);

// PostgreSQL
const pool = new Pool({ connectionString: CONFIG.POSTGRES_URL });

// Redis и очередь
const redis = new Redis(CONFIG.REDIS_URL);
const messageQueue = new Queue('messages', CONFIG.REDIS_URL);

// Утилиты
const normalizeFio = str => String(str).toLowerCase().replace(/\s+/g, ' ').trim();
const denormalizeFio = str => str.split(' ').map(s => s ? s[0].toUpperCase() + s.slice(1) : '').join(' ');
const isEmail = t => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(t).trim());
const isAdminId = id => CONFIG.ADMINS.includes(Number(id));

// Тексты
const TEMPLATES = {
  BANK_CONFIRMED:
    'Привет!\n' +
    'Банк подтвердил на почте присоединение к оферте.\n\n' +
    'Все формальные вопросы решены, жди обращения от бота по следующим шагам.\n' +
    'А также вступай в Альфа Клуб для студентов — карьерно-образовательную платформу ' +
    'для лучших студентов со всей страны  https://t.me/+bTj7nfzcjDNiNWEy',
  DOCS_NEED_FIX:
    'Привет!\n\n' +
    '📬 Команда Альфа-Банка проверила твои документы.\n' +
    '<b>Сейчас принять их не можем.</b>\n\n' +
    'Причина указана в письме на твою почту.\n\n' +
    'Что сделать дальше:\n' +
    '• Внеси правки по замечаниям из письма.\n' +
    '• Отправь обновлённый пакет на почту <b> alfa_chance@alfabank.ru</b>.\n\n' +
    'Как пришлёшь корректный комплект — мы оперативно перепроверим и вернёмся с подтверждением.'
};

const makeFaqText = () => (
  '<b>Часто задаваемые вопросы:</b>\n\n' +
  '1️⃣ <b>Когда будут результаты?</b>\n' +
  'Результаты публикации на сайте — после проверки документов (сентябрь).\n\n' +
  '2️⃣ <b>Какие документы нужны?</b>\n' +
  '• Заявка на присоединение к оферте (скан + .docx)\n' +
  '• Согласие на обработку персональных данных\n' +
  '• Копия паспорта\n' +
  '• Справка с места учёбы\n\n' +
  '3️⃣ <b>Куда отправлять документы?</b>\n' +
  'На почту: <b> alfa_chance@alfabank.ru</b>\n\n' +
  '4️⃣ <b>Нужен ли оригинал?</b>\n' +
  'Нет в письме, достаточно сканов.\n\n' +
  'Позже понадобятся оригиналы СОПД по почте, адрес и дату сообщим через бот.\n\n' +
  '5️⃣ <b>Как заполнять заявку к оферте?</b>\n' +
  'Можно и ручкой, можно и на компьютере, важно чтобы информация читалась.\n\n' +
  '6️⃣ <b>Как понять, что мои документы дошли?</b>\n' +
  'Вы получите подтверждение по email от команды программы.\n\n' +
  '7️⃣ <b>Могу ли узнать весь состав стипендиатов и когда?</b>\n' +
  'До официального объявления результатов на сайте данная информация конфиденциальна.'
);

// Работа с базой данных
async function getLookupSet() {
  const cacheKey = 'lookup_fio_set';
  const cached = await redis.get(cacheKey);
  if (cached) return new Set(JSON.parse(cached));
  
  const { rows } = await pool.query('SELECT normalized_fio FROM lookup');
  const set = new Set(rows.map(row => row.normalized_fio));
  await redis.set(cacheKey, JSON.stringify([...set]), 'EX', 86400); // 24 часа
  logger.info(`Lookup set cached, size: ${set.size}`);
  return set;
}

async function existsInLookupByFio(fioNormalized) {
  const set = await getLookupSet();
  return set.has(fioNormalized);
}

async function findProfileByChatId(chatId) {
  const { rows } = await pool.query('SELECT * FROM profiles WHERE chat_id = $1', [chatId]);
  return rows[0] || null;
}

async function findProfileByFio(fioNormalized) {
  const { rows } = await pool.query('SELECT * FROM profiles WHERE normalized_fio = $1', [fioNormalized]);
  return rows[0] || null;
}

async function ensureProfileRowByFio(fioNormalized, user) {
  let profile = await findProfileByFio(fioNormalized);
  if (!profile) {
    const { rows } = await pool.query(
      'INSERT INTO profiles (fio, normalized_fio, status, chat_id, username, first_name, last_name, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *',
      [denormalizeFio(fioNormalized), fioNormalized, 'Найден в списках (ожидает согласия)', user.chatId, user.username, user.firstName, user.lastName, new Date()]
    );
    profile = rows[0];
    logger.info(`Created new profile for ${fioNormalized}: id=${profile.id}`);
  }
  return profile;
}

// Состояния
async function setState(chatId, stateObj) {
  await redis.set(`state_${chatId}`, JSON.stringify(stateObj), 'EX', 86400);
}

async function getState(chatId) {
  const raw = await redis.get(`state_${chatId}`);
  return raw ? JSON.parse(raw) : null;
}

async function clearState(chatId) {
  await redis.del(`state_${chatId}`);
}

// Команда /start
bot.command('start', async ctx => {
  const chatId = ctx.chat.id;
  const user = {
    chatId,
    username: ctx.from.username || '',
    firstName: ctx.from.first_name || '',
    lastName: ctx.from.last_name || ''
  };
  
  const profile = await findProfileByChatId(chatId);
  if (profile) {
    await pool.query(
      'UPDATE profiles SET username = $1, first_name = $2, last_name = $3 WHERE chat_id = $4',
      [user.username, user.firstName, user.lastName, chatId]
    );
  }

  await ctx.reply(
    'Добро пожаловать в бот программы «Альфа-Будущее | Стипендии» 🎓✨\n\n' +
    'Этот бот создан, чтобы вы могли легко оформить документы на стипендию.\n' +
    'Продолжая взаимодействие с ботом, вы даёте своё Согласие на обработку персональных данных.',
    {
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [[{ text: '📄 Согласие на обработку персональных данных', url: CONFIG.POLICY_URL }]]
      }
    }
  );
  await ctx.reply(
    'Введите, пожалуйста, ваши ФИО (полностью, как в паспорте).\n\n<i>Пример: Иванов Иван Иванович</i>',
    { parse_mode: 'HTML' }
  );
});

// Команда /faq
bot.command('faq', async ctx => {
  await ctx.reply(makeFaqText(), { parse_mode: 'HTML' });
});

// Админ-команды
bot.command('bank_ok_row', async ctx => {
  if (!isAdminId(ctx.from.id)) {
    await ctx.reply('Недостаточно прав.');
    return;
  }
  const nums = ctx.message.text.match(/\d+/g) || [];
  if (!nums.length) {
    await ctx.reply('Укажите номер строки: /bank_ok_row 257');
    return;
  }

  const results = [];
  for (const id of nums) {
    const { rows } = await pool.query('SELECT * FROM profiles WHERE id = $1', [id]);
    const profile = rows[0];
    if (profile && profile.chat_id) {
      await ctx.telegram.sendMessage(profile.chat_id, TEMPLATES.BANK_CONFIRMED, { parse_mode: 'HTML' });
      await pool.query('UPDATE profiles SET status = $1 WHERE id = $2', ['Подтверждено банком (ждите следующих шагов)', id]);
      results.push(`строка ${id}: OK (chat_id=${profile.chat_id})`);
    } else {
      results.push(`строка ${id}: нет CHAT_ID`);
    }
  }
  await ctx.reply(`Результат:\n${results.join('\n')}`);
});

bot.command('bank_fix_row', async ctx => {
  if (!isAdminId(ctx.from.id)) {
    await ctx.reply('Недостаточно прав.');
    return;
  }

  const parseRows = s => {
    const set = new Set();
    for (const part of s.replace(/^[\/\\]bank_fix_row/i, '').trim().split(/[\s,]+/).filter(Boolean)) {
      if (part.includes('-')) {
        const [a, b] = part.split('-').map(x => parseInt(x, 10));
        if (Number.isInteger(a) && Number.isInteger(b)) {
          const from = Math.min(a, b), to = Math.max(a, b);
          for (let i = from; i <= to; i++) set.add(i);
        }
      } else {
        const n = parseInt(part, 10);
        if (Number.isInteger(n)) set.add(n);
      }
    }
    return [...set];
  };

  const rows = parseRows(ctx.message.text);
  if (!rows.length) {
    await ctx.reply('Укажите номер(а) строк: /bank_fix_row 12 18 25-30');
    return;
  }

  const ok = [], noChat = [], bad = [];
  for (const id of rows) {
    try {
      const { rows } = await pool.query('SELECT * FROM profiles WHERE id = $1', [id]);
      const profile = rows[0];
      if (profile && profile.chat_id) {
        await ctx.telegram.sendMessage(profile.chat_id, TEMPLATES.DOCS_NEED_FIX, { parse_mode: 'HTML' });
        await pool.query(
          'UPDATE profiles SET status = $1, last_action = $2, last_seen_ts = $3 WHERE id = $4',
          ['Документы требуют исправления', 'Запрошены исправления', new Date(), id]
        );
        ok.push(id);
      } else {
        noChat.push(id);
      }
    } catch (e) {
      bad.push(id);
      logger.error(`Error processing row ${id}: ${e}`);
    }
  }

  await ctx.reply(
    `Результат:\n` +
    (ok.length ? `✅ Отправлено: ${ok.join(', ')}\n` : '') +
    (noChat.length ? `⚠️ Нет CHAT_ID: ${noChat.join(', ')}\n` : '') +
    (bad.length ? `❌ Ошибка: ${bad.join(', ')}` : '')
  );
});

// Обработка текстовых сообщений
bot.on('text', async ctx => {
  const chatId = ctx.chat.id;
  const text = ctx.message.text.trim();
  logger.info(`Received message from ${chatId}: ${text}`);

  const user = {
    chatId,
    username: ctx.from.username || '',
    firstName: ctx.from.first_name || '',
    lastName: ctx.from.last_name || ''
  };

  // Проверка состояния (ожидание email)
  const state = await getState(chatId);
  if (state && state.await === 'email' && state.profileId) {
    if (!isEmail(text)) {
      await ctx.reply('Похоже, это не email. Пришлите адрес в формате <i>name@example.com</i>.', { parse_mode: 'HTML' });
      return;
    }
    await pool.query(
      'UPDATE profiles SET email = $1, docs_email = $2, docs_email_ts = $3, status = $4 WHERE id = $5',
      [text, 'Отправил', new Date(), 'Документы отправлены (ожидают проверку)', state.profileId]
    );
    await clearState(chatId);
    await ctx.reply(
      'Спасибо! Команда Альфа-Банка проверит ваши документы и свяжется с вами с почты <b> alfa_chance@alfabank.ru</b> в ближайшее время. ✅',
      { parse_mode: 'HTML' }
    );
    await ctx.reply(
      `<b>👉 Шаг 2.</b> Пожалуйста, заполни ту же информацию в короткой форме.\n${CONFIG.FORM_URL}`,
      {
        parse_mode: 'HTML',
        reply_markup: { inline_keyboard: [[{ text: 'Перейти к форме', url: CONFIG.FORM_URL }, { text: '✅ Опрос пройден', callback_data: `survey_done|${state.profileId}` }]] }
      }
    );
    return;
  }

  // Проверка email вне состояния
  if (isEmail(text)) {
    const profile = await findProfileByChatId(chatId);
    if (profile) {
      await pool.query(
        'UPDATE profiles SET email = $1, docs_email = $2, docs_email_ts = $3, status = $4 WHERE chat_id = $5',
        [text, 'Отправил', new Date(), 'Документы отправлены (ожидают проверку)', chatId]
      );
      await clearState(chatId);
      await ctx.reply(
        'Спасибо! Команда Альфа-Банка проверит ваши документы и свяжется с вами с почты <b> alfa_chance@alfabank.ru</b> в ближайшее время. ✅',
        { parse_mode: 'HTML' }
      );
      await ctx.reply(
        `<b>👉 Шаг 2.</b> Пожалуйста, заполни ту же информацию в короткой форме.\n${CONFIG.FORM_URL}`,
        {
          parse_mode: 'HTML',
          reply_markup: { inline_keyboard: [[{ text: 'Перейти к форме', url: CONFIG.FORM_URL }, { text: '✅ Опрос пройден', callback_data: `survey_done|${profile.id}` }]] }
        }
      );
    }
    return;
  }

  // Обработка ФИО
  const fioNorm = normalizeFio(text);
  if (fioNorm.length < 3) {
    await ctx.reply('Слишком коротко. Пришлите ФИО полностью.', { parse_mode: 'HTML' });
    return;
  }
  if (!(await existsInLookupByFio(fioNorm))) {
    await ctx.reply(
      'К сожалению, вы не являетесь рекомендованным претендентом, либо ФИО введено неверно. Если это уже не первая попытка - напишите на почту alfa_chance@alfabank.ru письмо с темой "Ошибка в боте_ФИО"\n\nПопробуйте ещё раз.',
      { parse_mode: 'HTML' }
    );
    return;
  }

  let profile = await findProfileByChatId(chatId);
  if (!profile) profile = await ensureProfileRowByFio(fioNorm, user);

  await ctx.reply(
    'Ура! Вы в списке претендентов на стипендию.\n\nДля получения статуса стипендиата ознакомьтесь с офертой (Приложение 4 Положения).',
    {
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [
          [{ text: 'Ознакомиться с офертой', url: CONFIG.OFFER_DOC_URL }],
          [
            { text: 'Согласен с офертой', callback_data: `agree|${profile.id}` },
            { text: 'Не согласен', callback_data: `decline|${profile.id}` }
          ]
        ]
      }
    }
  );
});

// Обработка callback-запросов
bot.on('callback_query', async ctx => {
  const chatId = ctx.chat.id;
  const [action, profileId] = ctx.callbackQuery.data.split('|');
  logger.info(`Callback from ${chatId}: ${action}|${profileId}`);

  if (action === 'faq') {
    await ctx.reply(makeFaqText(), { parse_mode: 'HTML' });
    await ctx.answerCbQuery('Принято');
    return;
  }

  if (!profileId) {
    await ctx.reply('Ошибка.');
    await ctx.answerCbQuery('Принято');
    return;
  }

  if (action === 'agree') {
    await pool.query(
      'UPDATE profiles SET status = $1, consent = $2, consent_ts = $3 WHERE id = $4 AND chat_id = $5',
      ['Оферта согласована (ожидает документы)', 'Да', new Date(), profileId, chatId]
    );
    await ctx.reply(
      'Отлично! ✅\n\n' +
      'Чтобы стать стипендиатом, нужно выполнить несколько шагов:\n\n' +
      '👉 Шаг 1. Отправить на почту <b> alfa_chance@alfabank.ru</b> пакет документов одним письмом до <b>30 сентября</b>...\n' +
      'Тема письма <b>"Стипендиат 2025 ФИО полностью"</b>',
      {
        parse_mode: 'HTML',
        reply_markup: {
          inline_keyboard: [
            [{ text: 'Заявка на присоединение к оферте', url: CONFIG.APPLICATION_URL }],
            [{ text: 'Согласие на обработку персональных данных', url: CONFIG.SOPD_URL }]
          ]
        }
      }
    );
    await ctx.reply(
      'После отправки документов нажмите кнопку ниже.\n\n' +
      '<i>Внимание: нажимайте кнопку «Я отправил(а) документы на почту!» только после того, как действительно отправили письмо с пакетом документов.</i>',
      {
        parse_mode: 'HTML',
        reply_markup: {
          inline_keyboard: [[{ text: '✅ Я отправил(а) документы на почту!', callback_data: `docs_sent|${profileId}` }]]
        }
      }
    );
    await ctx.answerCbQuery('Принято');
  } else if (action === 'decline') {
    await pool.query(
      'UPDATE profiles SET status = $1, consent = $2, consent_ts = $3 WHERE id = $4 AND chat_id = $5',
      ['Оферта отклонена', 'Нет', new Date(), profileId, chatId]
    );
    await ctx.reply(
      'К сожалению, без согласия с офертой вы не можете стать стипендиатом. Если решите изменить решение — просто нажмите на Согласен с офертой.',
      { parse_mode: 'HTML' }
    );
    await ctx.answerCbQuery('Принято');
  } else if (action === 'docs_sent') {
    await pool.query(
      'UPDATE profiles SET docs_email = $1, docs_email_ts = $2, status = $3 WHERE id = $4 AND chat_id = $5',
      ['Отправил', new Date(), 'Документы отправлены (ожидают проверку)', profileId, chatId]
    );
    await setState(chatId, { await: 'email', profileId });
    await ctx.reply(
      'Укажите, пожалуйста, <b>email</b>, с которого вы отправили документы (например, name@example.com).\n\n' +
      '<i>Если нажали кнопку по ошибке — можно отменить отметку.</i>',
      {
        parse_mode: 'HTML',
        reply_markup: {
          inline_keyboard: [[{ text: '❌ Ошибся, ещё не отправил(а) документы', callback_data: `docs_undo|${profileId}` }]]
        }
      }
    );
    await ctx.answerCbQuery('Принято');
  } else if (action === 'docs_undo') {
    await pool.query(
      'UPDATE profiles SET docs_email = NULL, docs_email_ts = NULL, status = $1 WHERE id = $2 AND chat_id = $3',
      ['Оферта согласована (ожидает документы)', profileId, chatId]
    );
    await clearState(chatId);
    await ctx.reply('Ок, отметку сняли. Нажмите «Я отправил(а) документы на почту!» после реальной отправки письма.', { parse_mode: 'HTML' });
    await ctx.answerCbQuery('Принято');
  } else if (action === 'survey_done') {
    await pool.query(
      'UPDATE profiles SET survey = $1, survey_ts = $2, status = $3, last_action = $4, last_seen_ts = $5 WHERE id = $6 AND chat_id = $7',
      ['Да', new Date(), 'Опрос пройден', 'Опрос завершён', new Date(), profileId, chatId]
    );
    await ctx.reply('Спасибо! ✅ Ваши ответы зафиксированы. Команда оргкомитета вернётся к вам по почте в ближайшее время.', { parse_mode: 'HTML' });
    await ctx.reply('Напоминаем, что по всем вопросам можно обращаться на официальную почту программы alfa_chance@alfabank.ru', { parse_mode: 'HTML' });
    await ctx.answerCbQuery('Принято');
  }
});

// Обработка очереди сообщений
messageQueue.process(async job => {
  const { chatId, text } = job.data;
  logger.info(`Processing queued message from ${chatId}: ${text}`);
  // Добавьте дополнительную логику обработки, если нужно
});

// Вебхук для Telegram
app.post('/webhook', async (req, res) => {
  try {
    const updateId = req.body.update_id;
    if (!updateId) {
      logger.warn('No update_id in webhook request');
      return res.status(200).send('OK');
    }
    
    const cacheKey = `update_${updateId}`;
    const cached = await redis.get(cacheKey);
    if (cached) {
      logger.info(`Duplicate update skipped: ${updateId}`);
      return res.status(200).send('OK');
    }
    
    await redis.set(cacheKey, '1', 'EX', 86400);
    await bot.handleUpdate(req.body);
    logger.info(`Processed update: ${updateId}`);
    res.status(200).send('OK');
  } catch (err) {
    logger.error(`Webhook error: ${err.stack}`);
    res.status(200).send('OK');
  }
});

// Проверка состояния вебхука
async function checkWebhook() {
  try {
    const response = await bot.telegram.getWebhookInfo();
    logger.info(`Webhook info: ${JSON.stringify(response)}`);
    if (!response.url || response.url !== CONFIG.WEBHOOK_URL || response.pending_update_count > 10) {
      logger.warn('Resetting webhook due to issues');
      await bot.telegram.deleteWebhook({ drop_pending_updates: true });
      await bot.telegram.setWebhook(CONFIG.WEBHOOK_URL);
      if (CONFIG.ADMINS[0]) {
        await bot.telegram.sendMessage(CONFIG.ADMINS[0], 'Webhook was reset due to issues');
      }
    }
  } catch (err) {
    logger.error(`Error checking webhook: ${err}`);
  }
}

// Запуск сервера
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  logger.info(`Server running on port ${PORT}`);
  await bot.telegram.setWebhook(CONFIG.WEBHOOK_URL);
  logger.info('Webhook set');
  // Периодическая проверка вебхука (каждые 10 минут)
  setInterval(checkWebhook, 10 * 60 * 1000);
});