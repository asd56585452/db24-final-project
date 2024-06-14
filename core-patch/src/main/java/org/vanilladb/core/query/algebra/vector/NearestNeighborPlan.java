package org.vanilladb.core.query.algebra.vector;

import static org.vanilladb.core.sql.RecordComparator.DIR_DESC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Logger;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.index.ivfflat.IvfflatIndex;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborPlan implements Plan {
    private static Logger logger = Logger.getLogger(NearestNeighborPlan.class.getName());
    private Plan child;
    private IndexInfo iia;
    private Transaction tx;
    private int limit;
    DistanceFn indexDistanceFn;

    public NearestNeighborPlan(String tblName, DistanceFn distFn, Transaction tx, int limit) {
        this.limit = limit;
        this.tx=tx;
        List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblName, distFn.fieldName(), tx);
        this.iia = null;
        for (IndexInfo ii : iis) {
            if(ii.indexType()==IndexType.IVFFLAT)
            {
                this.iia=ii;
                break;
            } 
        }
        String tblname = this.iia.indexName();
        Plan p = new TablePlan(tblname, tx);
        this.indexDistanceFn = new EuclideanFn(IvfflatIndex.SCHEMA_KEY);
        this.indexDistanceFn.setQueryVector(distFn.getQueryVector());
        if (limit>50 || limit ==-1)
            this.child =  new SortPlan(p, this.indexDistanceFn, tx);
        else
            this.child =  p;
    }

    @Override
    public Scan open() {
        
        // List<Scan> cs = new ArrayList<Scan>();
        // s.beforeFirst();
        // while(s.next())
        // {
        //     int index = (int)s.getVal(IvfflatIndex.SCHEMA_ID).asJavaVal();
        //     String ttblname = this.iia.indexName()+ "-"+ index + ".tbl";
		// 	TableInfo tti = VanillaDb.catalogMgr().getTableInfo(ttblname, this.tx);
        //     if(tti==null)
        //         continue;
        //     else
        //     {
        //         Plan ttp = new TablePlan(ttblname,this.tx);
        //         ttp = new SortPlan(ttp, this.indexDistanceFn, this.tx);
        //         cs.add(ttp.open());
        //     }
        // }
        if (limit>50 || limit ==-1)
        {
            Scan s = child.open();
            return new NearestNeighborScan(s,tx,iia,indexDistanceFn);
        }
        else
        {
            double min_dis = Double.MAX_VALUE;
            int min_index = 0;
            Scan is = child.open();
            is.beforeFirst();
            while(is.next())
            {
                VectorConstant v = (VectorConstant)is.getVal(IvfflatIndex.SCHEMA_KEY);
                double dis = indexDistanceFn.distance(v);
                if(dis<min_dis)
                {
                    min_dis=dis;
                    min_index = (Integer)is.getVal(IvfflatIndex.SCHEMA_ID).asJavaVal();
                }
            }
            is.close();
            String itblname = iia.indexName()+ "-"+ min_index;
            return executeCalculateRecall(iia.tableName(),itblname,indexDistanceFn,limit,tx);
        }
    }

    @Override
    public long blocksAccessed() {
        return child.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return child.schema();
    }

    @Override
    public Histogram histogram() {
        return child.histogram();
    }

    @Override
    public long recordsOutput() {
        return child.recordsOutput();
    }

    static class MapRecord implements Record{

		Map<String, Constant> fldVals = new HashMap<>();

		@Override
		public Constant getVal(String fldName) {
			return fldVals.get(fldName);
		}

		public void put(String fldName, Constant val) {
			fldVals.put(fldName, val);
		}

		public boolean containsKey(String fldName) {
			return fldVals.containsKey(fldName);
		}
	}

	static class PriorityQueueScan implements Scan {
        private PriorityQueue<MapRecord> pq;
        private boolean isBeforeFirsted = false;
        private TableScan ts;
        private Transaction tx;
        private String otableName;

        public PriorityQueueScan(PriorityQueue<MapRecord> pq,String otableName, Transaction tx) {
            this.pq = pq;
            this.ts = (TableScan)new TablePlan(otableName, tx).open();
            this.tx = tx;
            this.otableName = otableName;
        }

        @Override
        public Constant getVal(String fldName) {
            return ts.getVal(fldName);
        }

        @Override
        public void beforeFirst() {
            this.isBeforeFirsted = true;
            this.ts.beforeFirst();
        }

        @Override
        public boolean next() {
            if (isBeforeFirsted) {
                isBeforeFirsted = false;
                long blkNum = (Long) pq.peek().getVal(IvfflatIndex.SCHEMA_RID_BLOCK).asJavaVal();
                int id = (Integer) pq.peek().getVal(IvfflatIndex.SCHEMA_RID_ID).asJavaVal();
                RecordId rid = new RecordId(new BlockId(otableName+".tbl", blkNum), id);
                ts.moveToRecordId(rid);
                return true;
            }
            pq.poll();
            long blkNum = (Long) pq.peek().getVal(IvfflatIndex.SCHEMA_RID_BLOCK).asJavaVal();
            int id = (Integer) pq.peek().getVal(IvfflatIndex.SCHEMA_RID_ID).asJavaVal();
            RecordId rid = new RecordId(new BlockId(otableName+".tbl", blkNum), id);
            ts.moveToRecordId(rid);
            return pq.size() > 0;
        }

        @Override
        public void close() {
            this.ts.close();
            return;
        }

        @Override
        public boolean hasField(String fldName) {
            return pq.peek().containsKey(fldName);
        }
    }

	public static Scan executeCalculateRecall(String otableName,String tableName, DistanceFn distFn, int limit, Transaction tx) {
		Plan p = new TablePlan(tableName, tx);

		List<String> sortFlds = new ArrayList<String>();
		sortFlds.add(distFn.fieldName());
		
		List<Integer> sortDirs = new ArrayList<Integer>();
		sortDirs.add(DIR_DESC); // for priority queue

		RecordComparator comp = new RecordComparator(sortFlds, sortDirs, distFn);

		PriorityQueue<MapRecord> pq = new PriorityQueue<>(limit, (MapRecord r1, MapRecord r2) -> comp.compare(r1, r2));
		
		Scan s = p.open();
		s.beforeFirst();
		while (s.next()) {
			MapRecord fldVals = new MapRecord();
			for (String fldName : p.schema().fields()) {
				fldVals.put(fldName, s.getVal(fldName));
			}
			pq.add(fldVals);
			if (pq.size() > limit)
				pq.poll();
		}
		s.close();

		s = new PriorityQueueScan(pq,otableName,tx);

		return s;
	}
}       
