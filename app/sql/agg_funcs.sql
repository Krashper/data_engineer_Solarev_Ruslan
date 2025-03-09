-- Кол-во созданных аккаунтов
SELECT EXTRACT(DAY FROM created_at) AS p_day, COUNT(*)
FROM "Users"
GROUP BY p_day
ORDER BY p_day;


-- Информация о пользователях, написавших сообщения
SELECT u.*
FROM "Content" AS co
JOIN "Content_Log" AS cl
    ON cl.content_id = co.content_id
JOIN "Logs" AS l
    ON l.log_id = cl.log_id
JOIN "Users" AS u
    ON l.user_id = u.user_id
WHERE co.type = 'Сообщение' AND EXTRACT(day FROM co.created_at) = 1;


-- Кол-во написанных сообщений
SELECT EXTRACT(DAY FROM created_at) AS p_day, COUNT(*)
FROM "Content" co
WHERE co.type = 'Сообщение'
GROUP BY p_day
ORDER BY p_day;

-- Кол-во тем за предыдущие дни
SELECT COUNT(*)
FROM "Content" co
WHERE co.type = 'Тема' AND EXTRACT(DAY FROM created_at) < 10;

-- Кол-во тем за текущий день
SELECT COUNT(*)
FROM "Content" co
WHERE co.type = 'Тема' AND EXTRACT(DAY FROM created_at) = 10;
