declare
  m_ ifsapp.lpe_queue_msg;
begin
  m_ := ifsapp.lpe_msg_queue_api.dequeue(?, 0);
  ? := m_.data;
end;
