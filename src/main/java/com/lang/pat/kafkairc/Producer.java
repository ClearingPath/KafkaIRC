/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lang.pat.kafkairc;

/**
 *
 * @author ClearingPath
 */
public class Producer {
    public Producer(){
        
    }
    
    public void send(String Message){
        for (String c : ClientMain.ChannelList){
            send(Message,c);
        }
    }
    
    public void send(String Message, String Channel){
        
    }
}
