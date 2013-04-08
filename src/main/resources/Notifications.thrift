namespace java com.yahoo.omid.notifications.thrift.generated

struct Notification {
	1: string observer,
	2: binary table,
	3: binary rowKey,
	4: binary columnFamily,
	5: binary column
}

exception ObserverOverloaded {

}

service NotificationReceiverService {
	void notify(1: Notification notif) throws (1:ObserverOverloaded e)
}