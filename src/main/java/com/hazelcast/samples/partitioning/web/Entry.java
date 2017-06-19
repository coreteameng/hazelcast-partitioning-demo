package com.hazelcast.samples.partitioning.web;

import com.hazelcast.samples.partitioning.Styles;
import com.vaadin.ui.Button;

/**
 * Created by Alparslan on 7.06.2017.
 */
public class Entry extends Button {

    public Entry(String key) {
        setCaption(key);
        addStyleName(Styles.BUTTON_CAPTION_MARGIN);
        setHeight(35, Unit.PIXELS);
        setWidth(35, Unit.PIXELS);
    }
}
