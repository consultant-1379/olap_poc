//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vhudson-jaxb-ri-2.1-2 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2013.11.11 at 12:59:40 PM GMT 
//


package com.ericsson.nms.dg.schema;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for data_typeType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="data_typeType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="data-cardinality" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="data-new-val" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="supp-data-length" type="{http://www.w3.org/2001/XMLSchema}int" minOccurs="0"/>
 *         &lt;element name="max-data-length" type="{http://www.w3.org/2001/XMLSchema}int" minOccurs="0"/>
 *         &lt;element name="avg-data-length" type="{http://www.w3.org/2001/XMLSchema}int" minOccurs="0"/>
 *         &lt;element name="scale" type="{http://www.w3.org/2001/XMLSchema}int" minOccurs="0"/>
 *       &lt;/sequence>
 *       &lt;attribute name="type" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "data_typeType", namespace = "http://www.ericsson.com/dg", propOrder = {
    "dataCardinality",
    "dataNewVal",
    "suppDataLength",
    "maxDataLength",
    "avgDataLength",
    "scale"
})
public class DataTypeType {

    @XmlElement(name = "data-cardinality")
    protected int dataCardinality;
    @XmlElement(name = "data-new-val")
    protected int dataNewVal;
    @XmlElement(name = "supp-data-length", defaultValue = "0")
    protected Integer suppDataLength;
    @XmlElement(name = "max-data-length")
    protected Integer maxDataLength;
    @XmlElement(name = "avg-data-length")
    protected Integer avgDataLength;
    @XmlElement(defaultValue = "0")
    protected Integer scale;
    @XmlAttribute(name = "type")
    protected String type;

    /**
     * Gets the value of the dataCardinality property.
     * 
     */
    public int getDataCardinality() {
        return dataCardinality;
    }

    /**
     * Sets the value of the dataCardinality property.
     * 
     */
    public void setDataCardinality(int value) {
        this.dataCardinality = value;
    }

    /**
     * Gets the value of the dataNewVal property.
     * 
     */
    public int getDataNewVal() {
        return dataNewVal;
    }

    /**
     * Sets the value of the dataNewVal property.
     * 
     */
    public void setDataNewVal(int value) {
        this.dataNewVal = value;
    }

    /**
     * Gets the value of the suppDataLength property.
     * 
     * @return
     *     possible object is
     *     {@link Integer }
     *     
     */
    public Integer getSuppDataLength() {
        return suppDataLength;
    }

    /**
     * Sets the value of the suppDataLength property.
     * 
     * @param value
     *     allowed object is
     *     {@link Integer }
     *     
     */
    public void setSuppDataLength(Integer value) {
        this.suppDataLength = value;
    }

    /**
     * Gets the value of the maxDataLength property.
     * 
     * @return
     *     possible object is
     *     {@link Integer }
     *     
     */
    public Integer getMaxDataLength() {
        return maxDataLength;
    }

    /**
     * Sets the value of the maxDataLength property.
     * 
     * @param value
     *     allowed object is
     *     {@link Integer }
     *     
     */
    public void setMaxDataLength(Integer value) {
        this.maxDataLength = value;
    }

    /**
     * Gets the value of the avgDataLength property.
     * 
     * @return
     *     possible object is
     *     {@link Integer }
     *     
     */
    public Integer getAvgDataLength() {
        return avgDataLength;
    }

    /**
     * Sets the value of the avgDataLength property.
     * 
     * @param value
     *     allowed object is
     *     {@link Integer }
     *     
     */
    public void setAvgDataLength(Integer value) {
        this.avgDataLength = value;
    }

    /**
     * Gets the value of the scale property.
     * 
     * @return
     *     possible object is
     *     {@link Integer }
     *     
     */
    public Integer getScale() {
        return scale;
    }

    /**
     * Sets the value of the scale property.
     * 
     * @param value
     *     allowed object is
     *     {@link Integer }
     *     
     */
    public void setScale(Integer value) {
        this.scale = value;
    }

    /**
     * Gets the value of the type property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the value of the type property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setType(String value) {
        this.type = value;
    }

}
