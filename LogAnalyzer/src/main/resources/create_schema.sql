create table if not exists consumed_messages (
  jms_id         varchar(256) not null,
  outcome        char(1) not null,
  outcome_time   timestamp not null,
  consumed_time  timestamp not null,
  application_id varchar(256) null,
  payload_size   integer null
);

create index if not exists ix_cm_app_id on consumed_messages (application_id);
create index if not exists ix_cm_outcome_time on consumed_messages (outcome_time);
create index if not exists ix_cm_consumed_time on consumed_messages (consumed_time);

create table if not exists produced_messages (
  jms_id         varchar(256) not null,
  application_id varchar(256) null,
  outcome        char(1) not null,
  produced_time  timestamp not null,
  outcome_time   timestamp not null,
  payload_size   integer null,
  delay_seconds  integer not null,
  constraint pk_produced_messages primary key (jms_id)
);

create index if not exists ix_pm_app_id on produced_messages (application_id);
create index if not exists ix_pm_outcome_time on produced_messages (outcome_time);
create index if not exists ix_pm_produced_time on produced_messages (produced_time);

create view if not exists ghost_messages as
  select cm.* from consumed_messages cm
    join produced_messages pm on pm.application_id = cm.application_id
    where pm.outcome = 'R';

create view if not exists undead_messages as
  select * from consumed_messages cm
    where not exists (select * from produced_messages pm
      where pm.application_id = cm.application_id)
      and cm.application_id is not null;

create view if not exists alien_messages as
  select * from consumed_messages
    where application_id is null;

create view if not exists lost_messages as
  select * from produced_messages pm
    where pm.outcome = 'C'
      and pm.application_id is not null
      and not exists (select * from consumed_messages cm
      where cm.application_id = pm.application_id
        and cm.outcome = 'C');

create view if not exists duplicate_messages as
  select count(*) duplicates, application_id from consumed_messages
    where outcome = 'C'
    and application_id is not null
    group by application_id
    having count(*) > 1;

create view if not exists consumed_per_minute as
  select count(*) total_count, sum(payload_size) total_bytes,
         max(payload_size) max_size,
         avg(payload_size) average_size,
         min(payload_size) min_size,
         median(payload_size) median_size,
         trunc(outcome_time, 'mi') time_period
  from consumed_messages
  where outcome = 'C'
  group by trunc(outcome_time, 'mi');

create view if not exists produced_per_minute as
  select count(*) total_count, sum(payload_size) total_bytes,
         max(payload_size) max_size,
         avg(payload_size) average_size,
         min(payload_size) min_size,
         median(payload_size) median_size,
         trunc(outcome_time, 'mi') time_period
  from produced_messages
  where outcome = 'C'
  group by trunc(outcome_time, 'mi');  

create view if not exists messages_per_minute as
  select p.total_count produced_count,
         c.total_count consumed_count,
         p.max_size produced_max_size,
         c.max_size consumed_max_size,
         p.median_size produced_median_size,
         c.median_size consumed_median_size,
         nvl(p.time_period, c.time_period) time_period
   from produced_per_minute p
   full outer join consumed_per_minute c on p.time_period = c.time_period;
         
create view if not exists message_flight_time as
  select p.application_id, p.produced_time, c.consumed_time,
         datediff('millisecond', p.produced_time, c.consumed_time) flight_time_millis
  from produced_messages p
  join consumed_messages c on c.application_id = p.application_id
   and p.application_id is not null
  where p.outcome = 'C'
    and c.outcome = 'C';
