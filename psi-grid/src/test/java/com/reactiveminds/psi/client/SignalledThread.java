package com.reactiveminds.psi.client;

abstract class SignalledThread {

    synchronized void  pause(){
        try {
            wait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    synchronized void  resume(){
        notify();
    }
}
