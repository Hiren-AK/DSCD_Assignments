# RabbitMQ YouTube Notification System

Files moved from A1/Youtube_DSCD:
- YouTubeServer.py
- User.py
- Youtuber.py

Note: erl_crash.dump has been removed per repository cleanup.

How to run

```bash
pip install pika
cd 01-comm-messaging/rabbitmq_youtube_notifications
python YouTubeServer.py
python User.py <username> <action> <youtuber_name>
python Youtuber.py <youtuber_name> <video_name>
```
