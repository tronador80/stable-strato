package eu.stratosphere.pact.common.io;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.generic.io.OutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;


/**
 * 
 */
public abstract class GenericTableOutputFormat implements OutputFormat<PactRecord>
{
	public static final String JT_ID_KEY = "pact.hbase.jtkey";
	
	public static final String JOB_ID_KEY = "pact.job.id";
	
	
	private RecordWriter<ImmutableBytesWritable, KeyValue> writer;
	
	private Configuration config;
	
	private org.apache.hadoop.conf.Configuration hadoopConfig;
	
	private TaskAttemptContext context;
	
	private String jtID;
	
	private int jobId;

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.OutputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{
		this.config = parameters;
		
		// get the ID parameters
		this.jtID = parameters.getString(JT_ID_KEY, null);
		if (this.jtID == null) {
			throw new RuntimeException("Missing JT_ID entry in hbase config.");
		}
		this.jobId = parameters.getInteger(JOB_ID_KEY, -1);
		if (this.jobId < 0) {
			throw new RuntimeException("Missing or invalid job id in input config.");
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.OutputFormat#open(int)
	 */
	@Override
	public void open(int taskNumber) throws IOException
	{
		this.hadoopConfig = getHadoopConfig(this.config);
		final TaskAttemptID attemptId = new TaskAttemptID(this.jtID, this.jobId, false, taskNumber - 1, 0);
		
		this.context = new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(this.hadoopConfig, attemptId);
		final HFileOutputFormat outFormat = new HFileOutputFormat();
		try {
			this.writer = outFormat.getRecordWriter(this.context);
		} catch (InterruptedException iex) {
			throw new IOException("Opening the writer was interrupted.", iex);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.OutputFormat#close()
	 */
	@Override
	public void close() throws IOException
	{
		final RecordWriter<ImmutableBytesWritable, KeyValue> writer = this.writer;
		this.writer = null;
		if (writer != null) {
			try {
				writer.close(this.context);
			} catch (InterruptedException iex) {
				throw new IOException("Closing was interrupted.", iex);
			}
		}
	}
	
	public void collectKeyValue(KeyValue kv) throws IOException {
		try {
			this.writer.write(null, kv);
		} catch (InterruptedException iex) {
			throw new IOException("Write request was interrupted.", iex);
		}
	}
	
	public abstract org.apache.hadoop.conf.Configuration getHadoopConfig(Configuration config);
}
