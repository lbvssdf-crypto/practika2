UPDATE public.accounts
SET owner_name = 'Павел Соколов', city = 'Москва',
    effective_at = TIMESTAMP '2026-03-05 10:00:00'
WHERE account_id = 101;

UPDATE public.accounts
SET status = 'SUSPENDED', effective_at = TIMESTAMP '2026-03-07 12:00:00'
WHERE account_id = 101;

UPDATE public.accounts
SET status = 'CANCELLED', effective_at = TIMESTAMP '2026-03-09 18:00:00'
WHERE account_id = 101;

DELETE FROM public.accounts WHERE account_id = 102;

-- Позднее исправление поступает после отмены и имеет более раннее время действия.
-- Операционную строку восстанавливает следующий UPDATE в том же COMMIT.
UPDATE public.accounts
SET owner_name = 'Анна Романова', city = 'Самара', status = 'ACTIVE',
    effective_at = TIMESTAMP '2026-03-03 08:00:00'
WHERE account_id = 101;

UPDATE public.accounts
SET owner_name = 'Павел Соколов', city = 'Москва', status = 'CANCELLED',
    effective_at = TIMESTAMP '2026-03-09 18:00:00'
WHERE account_id = 101;

INSERT INTO public.accounts VALUES
    (103, 'Мария Белова', 'Пермь', 'ACTIVE', TIMESTAMP '2026-03-10 09:00:00');
