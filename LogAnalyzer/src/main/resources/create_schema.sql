create table consumed_messages (
  jms_id         varchar(256) not null,
  outcome        char(1) not null,
  outcome_time   timestamp not null,
  consumed_time  timestamp not null,
  application_id varchar(256) null,
  payload_size   integer null
);

create index ix_cm_app_id on consumed_messages (application_id);

create table produced_messages (
  application_id varchar(256) not null,
  outcome        char(1) not null,
  produced_time  timestamp not null,
  outcome_time   timestamp not null,
  payload_size   integer not null,
  delay_seconds  integer not null,
  constraint pk_produced_messages primary key (application_id)
);

create view if not exists ghost_messages as
  select * from consumed_messages cm
    where not exists (select * from produced_messages pm
      where pm.application_id = cm.application_id
        and pm.outcome = 'C')
      and cm.application_id is not null;

create view if not exists alien_messages as
  select * from consumed_messages
    where application_id is null;

create view if not exists lost_messages as
  select * from produced_messages pm
    where pm.outcome = 'C'
      and not exists (select * from consumed_messages cm
      where cm.application_id = pm.application_id
        and cm.outcome = 'C');

create view if not exists duplicate_messages as
  select count(*) duplicates, application_id from consumed_messages
    where outcome = 'C'
    group by application_id
    having count(*) > 1;

create view if not exists consumed_per_minute as
  select count(*) total_count, sum(payload_size) total_bytes,
         max(payload_size) max_size,
         avg(payload_size) average_size,
         min(payload_size) min_size,
         median(payload_size) median_size,
         trunc(outcome_time, 'mi') period
  from consumed_messages
  where outcome = 'C'
  group by trunc(outcome_time, 'mi');

create view if not exists produced_per_minute as
  select count(*) total_count, sum(payload_size) total_bytes,
         max(payload_size) max_size,
         avg(payload_size) average_size,
         min(payload_size) min_size,
         median(payload_size) median_size,
         trunc(outcome_time, 'mi') period
  from produced_messages
  where outcome = 'C'
  group by trunc(outcome_time, 'mi');
  