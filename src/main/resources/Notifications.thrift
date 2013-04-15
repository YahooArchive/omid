namespace java com.yahoo.omid.notifications.thrift.generated

struct Notification {
	1: string observer,
	2: binary rowKey
}

exception ObserverOverloaded {
	1: string observer
}

service NotificationReceiverService {
	void notify(1: Notification notif) throws (1:ObserverOverloaded e)
}