package com.dotcms.enterprise.priv;

import java.util.List;

import com.dotmarketing.business.APILocator;
import com.dotmarketing.business.CacheLocator;
import com.dotmarketing.business.DotCacheAdministrator;
import com.dotmarketing.db.DbConnectionFactory;
import com.dotmarketing.db.HibernateUtil;
import com.dotmarketing.exception.DotHibernateException;
import com.dotmarketing.menubuilders.RefreshMenus;
import com.dotmarketing.util.Logger;
import com.dotmarketing.velocity.DotResourceCache;

public class ClusterThreadImpl extends Thread implements ClusterThread  {

	protected ClusterThreadImpl() {
		
	}
   
    private boolean start=false;
    private boolean work=false;
    
    

	private int sleep=100;
	private int delay=0;
	
	private void finish(){
		work=false;
		start=false;
	}
	

	private void startProcessing(int sleep, int delay) {
	  	this.sleep=sleep;
	  	this.delay=delay;
	  	start=true;
	}
	
	private boolean die=false;

	public void run() {

		while (!die) {
    	if (start && !work) {
    	if (delay>0) {
    		Logger.info(this, "ClusterThread start delayed for " + delay + " millis.");
    		try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				Logger.error(this, e.getMessage(), e);		
			}
    		
    	} 
    	Logger.info(this, "ClusterThread started with a sleep of " + sleep);
    	start=false;
    	work=true;
    	}
    	try {
			Thread.sleep(10);
		} catch (InterruptedException e2) {
			Logger.error(this, e2.getMessage(), e2);
		}
    	if (work) {
    	try 
    	{
    		//Logger.error(this, "In work loop");
    		HibernateUtil.startTransaction();	
    		List<String> cacheEntriesToRemove = APILocator.getDistributedJournalAPI().findCacheEntriesToRemove();
    		boolean flushMenus = false;
    		DotResourceCache vc = CacheLocator.getVeloctyResourceCache();
    		String menuGroup = vc.getMenuGroup();
    		for (String string : cacheEntriesToRemove) {
				int i = string.lastIndexOf(":");
				if(i > 0){
					String key = string.substring(0, i);
					String group = string.substring(i+1, string.length());
					if(key.contains("dynamic")){
						if(group.equals(menuGroup)){
							flushMenus = true;
							continue;
						}
					}
					if(key.equals("0")){
						if(group.equals(DotCacheAdministrator.ROOT_GOUP)){
							CacheLocator.getCacheAdministrator().flushAlLocalOnly();
						}else if(group.equals(menuGroup)){
							flushMenus = true;
						}else{
							CacheLocator.getCacheAdministrator().flushGroupLocalOnly(group, true);
						}
					}else{
						CacheLocator.getCacheAdministrator().removeLocalOnly(key, group, true);
					}
				}else{
					Logger.error(this, "The cache to locally remove key is invalid. The value was " + string);
				}
			}
    		HibernateUtil.commitTransaction();
    		if(flushMenus){
    			RefreshMenus.deleteMenusOnFileSystemOnly();
    			CacheLocator.getCacheAdministrator().flushGroupLocalOnly(menuGroup, true);
    		}
    	}
    	catch (Exception e) 
    	{
    		Logger.error(this, "Error ocurred trying to review contents.", e);
    		try {
				HibernateUtil.rollbackTransaction();
			} catch (DotHibernateException e1) {
				Logger.error(this, e1.getMessage(), e1);
			}
    	}
    	finally
    	{
    		try {
				HibernateUtil.closeSession();
			} catch (DotHibernateException e) {
				Logger.error(this,e.getMessage(), e);
			}
    		DbConnectionFactory.closeConnection();
    	}
    	
    	//Move records from  dist_process  to dist_journal
    	try 
    	{
    		HibernateUtil.startTransaction();
    		APILocator.getDistributedJournalAPI().processJournalEntries();
    		HibernateUtil.commitTransaction();
    	}
    	catch (Exception e) 
    	{
    		Logger.error(this, "Error ocurred while trying to process journal entries.", e);
    		try {
				HibernateUtil.rollbackTransaction();
			} catch (DotHibernateException e1) {
				Logger.error(this, e1.getMessage(), e1);
			}
    	}
    	finally
    	{
    		try {
				HibernateUtil.closeSession();
			} catch (DotHibernateException e) {
				Logger.error(this,e.getMessage(), e);
			}
    		DbConnectionFactory.closeConnection();
    	}
    	
    	}
    	try {
			Thread.sleep(sleep);
		} catch (InterruptedException e) {
			Logger.error(this, e.getMessage(),e);
		}	
    	}
		
		Logger.info(this, "ClusterThread shutting down");
    	
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#destroy()
     */
    public void destroy() 
    {
        
    }
    
 
    
    private static ClusterThreadImpl instance;
    
    /**
     * Tells the thread to start processing. Starts the thread 
     */
    public synchronized static void startThread(int sleep, int delay) {
    	Logger.info(ClusterThreadImpl.class, "ContentIndexationThread ordered to start processing");
    	
    	if (instance==null) {
    		instance=new ClusterThreadImpl();
    		instance.start(); 
    	}
    		instance.startProcessing(sleep,delay);
    }
    
    /**
     * Tells the thread to finish what it's down and stop
     */
    public synchronized static void shutdownThread() {
		if (instance!=null && instance.isAlive()) {
			Logger.info(ClusterThreadImpl.class, "ClusterThread shutdown initiated");
			instance.die=true;
		} else {
			Logger.warn(ClusterThreadImpl.class, "ClusterThread not running (or already shutting down)");
		}
	}
    
    /**
     * Tells the thread to stop processing. Doesn't shut down the thread.
     */
    public synchronized static void stopThread() {
    	if (instance!=null  && instance.isAlive()) {
    		Logger.info(ClusterThreadImpl.class, "ContentIndexationThread ordered to stop processing");
        	instance.finish();
    	} else {
    		Logger.error(ClusterThreadImpl.class, "No ContentIndexationThread available");
        	
    	}
    }
    /**
     * Creates and starts a thread that doesn't process anything yet
     */
    public synchronized static void createThread() {
    	if (instance==null) {
    		instance=new ClusterThreadImpl();
    		instance.start(); 
    	}
    	
    }
}
