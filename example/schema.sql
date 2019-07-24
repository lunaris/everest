create table if not exists event
  ( _offset bigserial not null
  , _timestamp timestamptz not null default now()
  , topic text not null
  , key uuid not null
  , value jsonb not null

  , primary key (_offset)
  );

create or replace function notify_event() returns trigger as
$$
declare
begin
  perform pg_notify('event', new._offset :: text);
  return new;
end
$$ language plpgsql;

do
$$
begin
  create trigger notify_event_on_event_insert
  after insert on event
  for each row
  execute procedure notify_event();
exception
  when duplicate_object then null;
end;
$$;
