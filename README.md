YADTQ - Yet Another Distributed Task Queue
YADTQ is a scalable and fault-tolerant Distributed Task Queue designed for managing asynchronous tasks across multiple worker nodes. 
It utilizes KAFKA as the message broker and REDIS for backend storage to track task statuses and results.
 This system ensures smooth communication between clients and workers, enabling efficient load distribution and robust task processing.

Features
Task Management: Asynchronous task submission by clients with unique task IDs.
Task Distribution: Tasks are processed by multiple worker nodes using Kafka consumer groups.
Fault Tolerance: Reassigns tasks to healthy workers in case of node failures.
Status Tracking: Redis stores task statuses (queued, processing, success, failed) and results.
Heartbeat Monitoring: Tracks worker health for system reliability
