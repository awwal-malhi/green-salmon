package com.greensalmon.shared.notifications;

/** Sends notifications. Real implementations (e-mail, Teams, PagerDuty...) plug in here. */
public interface Notifier {

    void send(Notification notification);
}
