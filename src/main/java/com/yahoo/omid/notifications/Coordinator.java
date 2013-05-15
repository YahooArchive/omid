package com.yahoo.omid.notifications;

import com.google.common.net.HostAndPort;

public interface Coordinator {
    public void registerInstanceNotifier(HostAndPort hostAndPort, String app, String observer);

    public void registerAppSandbox(AppSandbox appSandbox) throws Exception;
}
