namespace java com.yahoo.omid.notifications.thrift.generated

struct Notification {
	1: binary rowKey
}

struct Started {
	1: string host,
	2: i32 port,
	3: string observer
}

service NotificationReceiverService {
	void serverStarted(1: Started started)
}

service NotificationService {
	list<Notification> getNotifications()
}