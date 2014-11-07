/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */
package com.vertica.squeal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LOPrinter;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigscript.parser.ParseException;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.util.List;

public class SquealParser extends GruntParser {
    private final static Log log = LogFactory.getLog(SquealParser.class);

    boolean verbose;
    boolean dryrun;
    boolean runPig;

    boolean haveStatements;

    int transCount;

    PigServer svr;
    LOTranslator trans;
    Connection conn;

    public SquealParser(Reader r, LOTranslator t, Connection c, 
			boolean v, boolean d, boolean p) {
	super(r);
	trans = t;
	conn = c;  	// can be null if it's a dry run
	verbose = v;
	dryrun = d;
	runPig = p;
	haveStatements = false;
	transCount = 0;
    }

    public void setParams(PigServer pigServer) {
	// intercept 'cause it's private above
	svr = pigServer;
	svr.setBatchOn();
	super.setParams(pigServer);
    }

    protected void translateAndRun() {
	if (!haveStatements) return;
	try {
	    long start = System.currentTimeMillis();
	    
	    if (runPig) { // run in pig instead
		svr.executeBatch();
		svr.discardBatch();
		haveStatements = false;
	    } else {

		LogicalPlan plan = svr.clonePlan(null);
		if (verbose) {
		    log.info("Dumping explain plan:");
		    LOPrinter lpp = new LOPrinter(System.err,plan);
		    lpp.setVerbose(true);
		    lpp.visit();
		}

		String jobName = svr.getPigContext().getProperties().getProperty(PigContext.JOB_NAME,
										 PigContext.JOB_NAME_PREFIX+":DefaultJobName");
		log.info("Translating...");
		List<Script> scripts = trans.run(jobName,plan,svr.getAliasKeySet(),transCount==0);
		transCount++;

		svr.discardBatch();
		svr.setBatchOn();
		haveStatements = false;

		ScriptRunner runner = new ScriptRunner(svr,conn);
		if (verbose) {
		    runner.dumpScripts(scripts, System.out);
		} 

		if (!dryrun) {
		    try {
			runner.runScripts(scripts,true);
		    } catch (Exception e) {
			runner.dumpScripts(scripts,System.out);
			throw e;
		    }
		}
	    }
	    long end = System.currentTimeMillis();
	    log.info("Total time: "+(end-start)+"ms");
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }

    public void cleanup() {
	translateAndRun();
    }

    @Override
    protected void processDescribe(String alias) throws IOException {
	// disable
    }

    @Override
    protected void processExplain(String alias, String script, boolean isVerbose, 
                                  String format, String target, 
                                  List<String> params, List<String> files) 
        throws IOException, ParseException {
	// disable
    }

    @Override
    protected void printAliases() throws IOException {
	// disable
    }

    // pass through processRegister

    @Override
    protected void processScript(String script, boolean batch, 
                                 List<String> params, List<String> files) 
        throws IOException, ParseException {
	// disable
    }

    // pass through processSet

    @Override
    protected void processCat(String path) throws IOException
    {
	translateAndRun(); // execute batch before GruntParser tries to do so
	super.processCat(path);
    }

    // pass through CD

    @Override
    protected void processDump(String alias) throws IOException
    {
	// disable
    }

    @Override
    protected void processIllustrate(String alias) throws IOException
    {
	// disable
    }

    @Override
    protected void processKill(String jobid) throws IOException
    {
	// disable
    }

    // pass through LS
    // pass through PWD
    // pass through Help ???

    @Override
    protected void processMove(String src, String dst) throws IOException
    {
	translateAndRun(); 
	super.processMove(src, dst);
    }

    @Override
    protected void processCopy(String src, String dst) throws IOException
    {
	translateAndRun(); 
	super.processMove(src, dst);
    }

    @Override
    protected void processCopyToLocal(String src, String dst) throws IOException
    {
	translateAndRun(); 
	super.processMove(src, dst);
    }

    @Override
    protected void processCopyFromLocal(String src, String dst) throws IOException
    {
	translateAndRun(); 
	super.processMove(src, dst);
    }

    // pass through Mkdir

    @Override
    protected void processPig(String cmd) throws IOException
    {
	haveStatements = true;
	super.processPig(cmd);
    }

    @Override
    protected void processRemove(String path, String options ) throws IOException
    {
	translateAndRun(); 
	super.processMove(path, options);
    }

    // pass through FsCommand
}