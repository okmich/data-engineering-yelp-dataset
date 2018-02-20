/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.hbase;

import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author datadev
 */
public class Util {
    

    public static byte[] as(String obj) {
        return Bytes.toBytes(obj);
    }

    public static byte[] as(long obj) {
        return Bytes.toBytes(obj);
    }

    public static byte[] as(double obj) {
        return Bytes.toBytes(obj);
    }
}
