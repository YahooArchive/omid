namespace java com.yahoo.omid.notifications.thrift.generated

struct Notification {
	1: binary rowKey
}

service NotificationService {
	list<Notification> getNotifications()
}