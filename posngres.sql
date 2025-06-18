-- Создание схемы таблиц

create table users (
    id serial primary key,
    name text,
    email text,
    role text,
    updated_at timestamp default current_timestamp
);

create table users_audit (
    id serial primary key,
    user_id integer,
    changet_at timestamp default current_timestamp,
    changet_by text,
    field_changet text,
    old_value text,
    new_value text
);

-- Добавление данных в таблицы

insert into users (name, email, role) values
('Ivan Ivanov', 'ivanov@mail.com', 'administrator'),
('Petr Petrov', 'petrov@jobmail.com', 'developer'),
('Aleksander Aleksandrov', 'aleksandrov@jobmail.com', 'manager'),
('Alla Pavlova', 'pavlova@jobmail.com', 'analyst'),
('Anna Isaeva', 'isaeva@jobmail.com', 'CEO')

-- Создание триггерной функции и триггера для автоматизации логирования изменений в users

create or replace function log_user_update()
returns trigger as $$
begin
    if new.name is distinct from old.name then
        insert into users_audit(user_id, changet_by, field_changet, old_value, new_value)
        values (old.id, current_user, 'name', old.name, new.name);
    end if;
    if new.email is distinct from old.email then
        insert into users_audit(user_id, changet_by, field_changet, old_value, new_value)
        values (old.id, current_user, 'email', old.email, new.email);
    end if;
    if new.role is distinct from old.role then
        insert into users_audit(user_id, changet_by, field_changet, old_value, new_value)
        values (old.id, current_user, 'role', old.role, new.role);
    end if;
    RETURN NEW;
END;
$$ language plpgsql;

create trigger trigger_log_user_update
before update on users
for each row
execute function log_user_update();

-- Установка расширения cron

CREATE EXTENSION IF NOT exists pg_cron

-- создание функции для экспорта данных из users_audit в файл csv

CREATE OR REPLACE FUNCTION export_audit()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    filename TEXT;
BEGIN
    filename := E'/tmp/users_audit_export_' ||
                CURRENT_TIMESTAMP || E'.csv';

    EXECUTE format(
        E'COPY (SELECT *
        FROM users_audit
        WHERE change_at::date = CURRENT_DATE)
        TO %L
        DELIMITER '',''
        CSV HEADER',
        filename
    );

    RETURN filename;
END
$$;

-- Создание задачи для pg_cron на экспорт данных в 3:00

SELECT cron.schedule('export_audit', '0 3 * * *',
$$
export_audit()
$$);

-- Проверка задачи в cron.job

select * from cron.job j



