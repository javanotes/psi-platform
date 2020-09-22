package com.reactiveminds.psi.common.util;

public class StopWatch {

	public enum State{
		STOPPED,STARTED,PAUSED,RESUMED
	}
	public StopWatch() {
	}

	
	private long startTime;
	private long stopTime;
	public void start() {
		startTime = System.currentTimeMillis();
	}
	public boolean isExpired(long timeout) {
		return System.currentTimeMillis() - startTime >= timeout;
	}
	private long pauseStart = 0;
	public void stop() {
		stopTime = System.currentTimeMillis();
	}
	public long elapsed() {
		return stopTime-startTime;
	}
	public void reset() {
		startTime = -1;
		stopTime = -1;
		pauseStart = 0;
	}
	public void pause() {
		pauseStart = System.currentTimeMillis();
	}
	public void resume() {
		if (pauseStart != 0) {
			startTime += (System.currentTimeMillis() - pauseStart);
			pauseStart = 0;
		}
	}
}
