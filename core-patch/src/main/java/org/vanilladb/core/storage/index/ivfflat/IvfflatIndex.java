/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.index.ivfflat;

import static java.sql.Types.NULL;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.VectorType;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

/**
 * A static hash implementation of {@link Index}. A fixed number of buckets is
 * allocated, and each bucket is implemented as a file of index records.
 */
public class IvfflatIndex extends Index {
	private static Logger logger = Logger.getLogger(IvfflatIndex.class.getName());
	/**
	 * A field name of the schema of index records.
	 */
	public static final String SCHEMA_KEY = "i_emb", SCHEMA_ID = "i_id", SCHEMA_RID_BLOCK = "block",SCHEMA_RID_ID = "id";

    public static final int NUM_BUCKETS;

	static {
		NUM_BUCKETS = CoreProperties.getLoader().getPropertyAsInteger(
            IvfflatIndex.class.getName() + ".NUM_BUCKETS", 100);
	}

	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
		int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
        int index_rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(index_schema(keyType));
        if(NUM_BUCKETS == 0)
            return 0;
		return (totRecs / rpb) / NUM_BUCKETS + NUM_BUCKETS/index_rpb;
	}

	/**
	 * Returns the schema of the index records.
	 * 
	 * @param fldType
	 *            the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */
	private static Schema schema(SearchKeyType keyType) {
		Schema sch = new Schema();
		sch.addField(SCHEMA_KEY, keyType.get(0));
		sch.addField(SCHEMA_RID_BLOCK, BIGINT);
		sch.addField(SCHEMA_RID_ID, INTEGER);
		return sch;
	}
    private static Schema index_schema(SearchKeyType keyType) {
		Schema sch = new Schema();
		sch.addField(SCHEMA_KEY, keyType.get(0));
		sch.addField(SCHEMA_ID, INTEGER);
		return sch;
	}
	
	private SearchKey searchKey;
    private RecordFile Irf = null;
	private RecordFile rf;
	private boolean isBeforeFirsted;
	private String logger_tblname;
	private int logger_index;
	private static VectorConstant[] vactorindexs = new VectorConstant[NUM_BUCKETS];
	private static boolean hasLoad = false;
	private float [] MAX_VECTOR;
	private VectorConstant MAX_VECTOR_CONS;

	/**
	 * Opens a hash index for the specified index.
	 * 
	 * @param ii
	 *            the information of this index
	 * @param keyType
	 *            the type of the search key
	 * @param tx
	 *            the calling transaction
	 */
	public IvfflatIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		super(ii, keyType, tx);
        if(keyType.length()!=1)
            throw new UnsupportedOperationException();
		String tblname = ii.indexName();
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblname, tx);
		VectorType vt=(VectorType)keyType.get(0);
		MAX_VECTOR = new float[vt.getArgument()];
		for(int i=0;i<vt.getArgument();i++)
		{
			MAX_VECTOR[i]=Float.MAX_VALUE;
		}
		MAX_VECTOR_CONS = new VectorConstant(MAX_VECTOR);
		if(ti!=null)
		{
			this.Irf = ti.open(tx, false);
			preLoadToMemory();
		}
		// if(ti==null)
		// {
		// 	VanillaDb.catalogMgr().createTable(tblname, index_schema(keyType), tx);
		// 	ti = VanillaDb.catalogMgr().getTableInfo(tblname, tx);
		// 	this.Irf = ti.open(tx, false);
        //     Irf.beforeFirst();
		// 	logger.info("ivfvlat initing...");
        //     Ivfflatinit();
		// }
		
	}

	@Override
	public void preLoadToMemory() {
		if(hasLoad)
			return;
		Irf.beforeFirst();
		Integer index_num = 0;
		for(int i=0;i<NUM_BUCKETS;i++)
		{
			vactorindexs[i] = MAX_VECTOR_CONS;
		}
		while (Irf.next()) {
			VectorConstant v = (VectorConstant)Irf.getVal(SCHEMA_KEY);
			index_num = (Integer)Irf.getVal(SCHEMA_ID).asJavaVal();
			vactorindexs[index_num] = v;
		}
		Irf.close();
		hasLoad=true;
	}

	/**
	 * Positions the index before the first index record having the specified
	 * search key. The method hashes the search key to determine the bucket, and
	 * then opens a {@link RecordFile} on the file corresponding to the bucket.
	 * The record file for the previous bucket (if any) is closed.
	 * 
	 * @see Index#beforeFirst(SearchRange)
	 */
	@Override
	public void beforeFirst(SearchRange searchRange) {
		if(!hasLoad)
			throw new IllegalStateException("You must call TrainIndex() before iterating index '"
			+ ii.indexName() + "'");
		close();
		// support the equality query only
		if (!searchRange.isSingleValue())
			throw new UnsupportedOperationException();

		this.searchKey = searchRange.asSearchKey();
		// new TableInfo(tblname, index_schema(keyType));

		

        EuclideanFn edf = new EuclideanFn(SCHEMA_KEY);
        edf.setQueryVector((VectorConstant)this.searchKey.get(0));
        double min_dist = Double.MAX_VALUE;
        Integer min_index = 0;

        for(Integer index_num = 0;index_num<NUM_BUCKETS;index_num++ )
        {
            VectorConstant v = vactorindexs[index_num];
            double dist = edf.distance(v);
            if(dist<min_dist)
            {
                min_dist = dist;
                min_index = index_num;
            }
        }

        //index-bucket

        String tblname = ii.indexName()+ "-"+ min_index;
		logger_tblname = tblname;
		logger_index = min_index;
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblname, tx);
		if(ti==null)
		{
			VanillaDb.catalogMgr().createTable(tblname, schema(keyType), tx);
			ti = VanillaDb.catalogMgr().getTableInfo(tblname, tx);
			this.rf = ti.open(tx, false);
		}
		else
		{
			this.rf = ti.open(tx, false);
		}
        // ti = new TableInfo(tblname, schema(keyType));
        
		rf.beforeFirst();
		
		isBeforeFirsted = true;
	}

	/**
	 * Moves to the next index record having the search key.
	 * 
	 * @see Index#next()
	 */
	@Override
	public boolean next() {
		if (!isBeforeFirsted)
			throw new IllegalStateException("You must call beforeFirst() before iterating index '"
					+ ii.indexName() + "'");
		
		while (rf.next())
			if (getKey().equals(searchKey))
				return true;
		return false;
	}

	/**
	 * Retrieves the data record ID from the current index record.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
		int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
		return new RecordId(new BlockId(dataFileName, blkNum), id);
	}

	/**
	 * Inserts a new index record into this index.
	 * 
	 * @see Index#insert(SearchKey, RecordId, boolean)
	 */
	@Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		if(!hasLoad)
			return;
		// long ti = java.lang.System.nanoTime();
		// search the position
		beforeFirst(new SearchRange(key));

		// ti = java.lang.System.nanoTime();
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		// insert the data
		rf.insert();
		rf.setVal(SCHEMA_KEY, key.get(0));
		rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
				.number()));
		rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
		// rf.beforeFirst();
		rf.close();
		// logger.info("rf:"+logger_tblname+":con:"+con+":SCHEMA_KEY:"+key.get(0).hashCode());
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
		// logger.info("t2:"+(java.lang.System.nanoTime()-ti));
		// ti = java.lang.System.nanoTime();
	}

	/**
	 * Deletes the specified index record.
	 * 
	 * @see Index#delete(SearchKey, RecordId, boolean)
	 */
	@Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		if(!hasLoad)
			return;
		// search the position
		beforeFirst(new SearchRange(key));
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		// delete the specified entry
		while (next())
			if (getDataRecordId().equals(dataRecordId)) {
				rf.delete();
				return;
			}
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
	}

	/**
	 * Closes the index by closing the current table scan.
	 * 
	 * @see Index#close()
	 */
	@Override
	public void close() {
		if (rf != null)
			rf.close();
	}

	private long fileSize(String fileName) {
		tx.concurrencyMgr().readFile(fileName);
		return VanillaDb.fileMgr().size(fileName);
	}
	
	private SearchKey getKey() {
		Constant[] vals = new Constant[keyType.length()];
		vals[0] = rf.getVal(SCHEMA_KEY);
		return new SearchKey(vals);
	}

    private void Ivfflatinit() {
        VectorType vt=(VectorType)keyType.get(0);
        for(int i=0;i<NUM_BUCKETS;i++)
        {
            Irf.insert();
            Irf.setVal(SCHEMA_ID,new IntegerConstant(i));
            Irf.setVal(SCHEMA_KEY,new VectorConstant(vt.getArgument()));
        }
    }
	

	public void TrainIndex() {
		long st=java.lang.System.currentTimeMillis();
		long endt=1000*60*27;
		float sample_rate=0.75f;
		int drop_index_minnum=50;
		hasLoad=true;
		close();
		//清空
		String tblname = ii.indexName();
		String fldname = ii.fieldNames().get(0);
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblname, tx);
		if(ti==null)
		{
			VanillaDb.catalogMgr().createTable(tblname, index_schema(keyType), tx);
			ti = VanillaDb.catalogMgr().getTableInfo(tblname, tx);
			this.Irf = ti.open(tx, false);
            Irf.beforeFirst();
			logger.info("ivfvlat initing...");
            Ivfflatinit();
		}
		// new TableInfo(tblname, index_schema(keyType));
		for(int index=0;index<NUM_BUCKETS;index++)
        {
			String ttblname = ii.indexName()+ "-"+ index;
			TableInfo tti = VanillaDb.catalogMgr().getTableInfo(ttblname, tx);
			if(tti==null)
			{
				continue;
			}
			else
			{
				// VanillaDb.catalogMgr().dropTable(ttblname, tx);
				RecordFile rf = tti.open(tx, false);
				rf.beforeFirst();
				while (rf.next()) {
					rf.delete();
				}
				logger.info(ttblname+":drop");
				rf.close();
			}
        }
		//初始kmean
		ti = VanillaDb.catalogMgr().getTableInfo(ii.tableName(), tx);
		Irf.beforeFirst();
		RecordFile trf = ti.open(tx, false);
		trf.beforeFirst();
		int index_num=0;
		int trf_count=0;
		while (trf.next())
		{
			if(Math.random()>sample_rate)
			{
				// logger.info("sample");
				continue;
			}
			String ttblname = ii.indexName()+ "-"+ index_num;
			TableInfo tti = VanillaDb.catalogMgr().getTableInfo(ttblname, tx);
			if(tti==null)
			{
				VanillaDb.catalogMgr().createTable(ttblname, schema(keyType), tx);
				tti = VanillaDb.catalogMgr().getTableInfo(ttblname, tx);
			}
			RecordFile rf = tti.open(tx, false);
			rf.insert();
			rf.setVal(SCHEMA_KEY, trf.getVal(fldname));
			rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(trf.currentRecordId().block()
					.number()));
			rf.setVal(SCHEMA_RID_ID, new IntegerConstant(trf.currentRecordId().id()));
			rf.close();
			index_num=(index_num+1)%NUM_BUCKETS;
			trf_count+=1;
		}
		int mean_count = (trf_count+NUM_BUCKETS-1)/NUM_BUCKETS;
		//計算kmean
		// int epoch = 5;
		long epocht=0;
		long insertt=0;
		for(int i=0;true;i++)
		{
			long tst=java.lang.System.currentTimeMillis();
			logger.info("training "+i);
			boolean breakkmean = true;
			LinkedList<Constant> out_means = new LinkedList<Constant>();
			int out_count = -1;
			Constant out_sum = null;
			for(int index=0;index<NUM_BUCKETS;index++)
			{
				Constant sum=null;
				int count=-1;
				Constant mean = vactorindexs[index];
				String ttblname = ii.indexName()+ "-"+ index;
				TableInfo tti = VanillaDb.catalogMgr().getTableInfo(ttblname, tx);
				if(tti==null)
				{
					continue;
				}
				else
				{
					// VanillaDb.catalogMgr().dropTable(ttblname, tx);
					RecordFile rf = tti.open(tx, false);
					rf.beforeFirst();
					while (rf.next()) {
						if(count==-1)
						{
							sum = rf.getVal(SCHEMA_KEY);
							count = 1;
						}
						else
						{
							sum = sum.add(rf.getVal(SCHEMA_KEY));
							count +=1;
						}
						if(count>mean_count)
						{
							if(out_count==-1)
							{
								out_sum = rf.getVal(SCHEMA_KEY);
								out_count=1;
							}
							else
							{
								out_sum = out_sum.add(rf.getVal(SCHEMA_KEY));
								out_count+=1;
							}
							if(out_count>=mean_count)
							{
								out_means.add(out_sum.div(new IntegerConstant(out_count)));
								out_count=-1;
							}
						}
						rf.delete();
					}
					if(sum!=null)
					{
						mean = sum.div(new IntegerConstant(count));
						// logger.info(mean.toString());
						if(!mean.equals(vactorindexs[index]))
						{
							breakkmean=false;
							vactorindexs[index]=(VectorConstant)mean;
						}	
					}
					if(count<drop_index_minnum*sample_rate)
					{
						breakkmean=false;
						vactorindexs[index]= MAX_VECTOR_CONS;
					}
					rf.close();
				}
			}
			//balance mean
			for(int index=0;index<NUM_BUCKETS;index++)
			{
				if(MAX_VECTOR_CONS.equals(vactorindexs[index]) && !out_means.isEmpty())
				{
					vactorindexs[index] = (VectorConstant)out_means.pop();
					// logger.info("balance mean:"+vactorindexs[index].toString());
				}
			}
			if((java.lang.System.currentTimeMillis()-st)>endt-epocht-insertt/sample_rate)
			{
				Map<String, Constant> fldValMap = new HashMap<String, Constant>();
				trf.beforeFirst();
				while (trf.next())
				{
					fldValMap.put(fldname, trf.getVal(fldname));
					insert(new SearchKey(ii.fieldNames(), fldValMap), trf.currentRecordId(), true);
				}
				break;
			}
			//重新填入
			insertt=java.lang.System.currentTimeMillis();
			Map<String, Constant> fldValMap = new HashMap<String, Constant>();
			trf.beforeFirst();
			while (trf.next())
			{
				if(Math.random()>sample_rate)
					continue;
				fldValMap.put(fldname, trf.getVal(fldname));
				insert(new SearchKey(ii.fieldNames(), fldValMap), trf.currentRecordId(), true);
			}
			insertt = java.lang.System.currentTimeMillis()-insertt;
			epocht = java.lang.System.currentTimeMillis()-tst;
		}
		logger.info("after training");
		Irf.beforeFirst();
		for(int index=0;index<NUM_BUCKETS;index++)
		{
			Irf.next();
			Irf.setVal(SCHEMA_KEY, vactorindexs[index]);
			Irf.setVal(SCHEMA_ID, new IntegerConstant(index));
			String ttblname = ii.indexName()+ "-"+ index;
			TableInfo tti = VanillaDb.catalogMgr().getTableInfo(ttblname, tx);
			if(tti==null)
			{
				Irf.delete();
				continue;
			}
			else
			{
				// VanillaDb.catalogMgr().dropTable(ttblname, tx);
				RecordFile rf = tti.open(tx, false);
				int con=0;
				rf.beforeFirst();
				while (rf.next()) {
					con++;
				}
				logger.info(ttblname+":con:"+con);
				rf.close();
				if(con<drop_index_minnum)
				{
					Irf.delete();
				}
			}
		}
		trf.close();
		Irf.close();
		// VanillaDb.catalogMgr().dropTable(ttblname, tx);
	}


}
