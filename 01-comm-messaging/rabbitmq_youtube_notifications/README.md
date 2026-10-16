# RabbitMQ YouTube Notification System

Overview

A small publish/subscribe notification pipeline using RabbitMQ for event delivery: YouTubeServer processes uploads and notifies subscribed users.

Stack
- Python 3.x
- RabbitMQ broker (pika client)

Key files
- YouTubeServer.py — central server that publishes notifications
- User.py — subscriber / user commands
- Youtuber.py — publisher script to simulate uploads

How to run

1) Start a RabbitMQ broker locally (or use cloud RabbitMQ).
2) Install pika:

```bash
pip install pika
```

3) Start server and use the User/Youtuber scripts:

```bash
python YouTubeServer.py
python User.py <username> <action> <youtuber_name>
python Youtuber.py <youtuber_name> <video_name>
```

Resume-ready summary
- Built a RabbitMQ-based publish/subscribe notification pipeline for video uploads and subscriber notifications.
