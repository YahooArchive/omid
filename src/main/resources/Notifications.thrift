namespace java com.yahoo.omid.notifications.thrift.generated

struct Notification {
	1: string observer,
	2: binary table,
	3: binary rowKey,
	4: binary columnFamily,
	5: binary column
}

service NotificationReceiverService {
	void notify(1: Notification notif)
}