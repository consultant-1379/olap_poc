package com.ericsson.nms.dg.jmx;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.ReflectionException;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 31/10/13
 * Time: 14:52
 */
public abstract class BaseMBean implements DynamicMBean {

  abstract Object getAttributeValue(final String attName);

  @Override
  public Object getAttribute(final String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
    return getAttributeValue(attribute);
  }

  @Override
  public void setAttribute(final Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
  }

  @Override
  public AttributeList getAttributes(final String[] attributes) {
    final AttributeList values = new AttributeList(attributes.length);
    for (String attName : attributes) {
      values.add(new Attribute(attName, getAttributeValue(attName)));
    }
    return values;
  }

  @Override
  public AttributeList setAttributes(final AttributeList attributes) {
    return null;
  }

  @Override
  public Object invoke(final String actionName, final Object[] params, final String[] signature) throws MBeanException, ReflectionException {
    return null;
  }
}
