# Cluster mechanism 

- Client sends job submission to frontend
- Job submitted to all the nodes in the cluster
- When a job is handed to a worker then a lock message is sent to all nodes
- A job can only be locked if nobody is working on it 
- Only *after* the job is locked is it given to the worker (this needs to be fast) 
-- all nodes receive a lock request and then say OK (this should be O(1))
- When a worker completes / fails a task, the completion is sent to all nodes to invalidate the job 
- Each node has its own data storage mechanism 
