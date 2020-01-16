select 
  b.*, 
  w.pid as blocked_pid, 
  w.txn_owner as blocked_owner, 
  datediff(minutes, b.txn_start, getdate()) as blocked_for_mints
from svv_transactions b
inner join svv_transactions w 
    on b.txn_db = w.txn_db and b.relation = w.relation
where b.granted='t' and w.granted = 'f' 
  and datediff(minutes, b.txn_start, getdate()) > 5
order by txn_start, b.pid;
