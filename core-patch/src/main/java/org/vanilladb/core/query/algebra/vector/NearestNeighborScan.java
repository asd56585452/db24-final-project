package org.vanilladb.core.query.algebra.vector;

import java.util.List;
import java.util.logging.Logger;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.ivfflat.IvfflatIndex;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborScan implements Scan {
    private static Logger logger = Logger.getLogger(NearestNeighborScan.class.getName());

    TableScan ts;
    Scan s;
    Scan cs=null;
    Transaction tx;
    IndexInfo iia;
    DistanceFn indexDistanceFn;
    String ttblname;

    public NearestNeighborScan(Scan s, Transaction tx,IndexInfo iia, DistanceFn indexDistanceFn) {
        this.ts = (TableScan)new TablePlan(iia.tableName(), tx).open();
        this.s = s;
        this.tx = tx;
        this.iia = iia;
        this.indexDistanceFn = indexDistanceFn;
    }

    @Override
    public void beforeFirst() {
        ts.beforeFirst();
        s.beforeFirst();
        this.set_cs();
    }

    @Override
    public boolean next() {
        while(true)
        {
            if(cs==null)
            {
                // logger.info("cs==null");
                return false;
            }
            if(cs.next())
            {
                long blkNum = (Long) cs.getVal(IvfflatIndex.SCHEMA_RID_BLOCK).asJavaVal();
                int id = (Integer) cs.getVal(IvfflatIndex.SCHEMA_RID_ID).asJavaVal();
                RecordId rid = new RecordId(new BlockId(iia.tableName()+".tbl", blkNum), id);
                ts.moveToRecordId(rid);
                return true;
            }
            else
            {
                // logger.info(tx.toString()+":next:cs no next");
                this.set_cs();
            } 
        }
    }

    @Override
    public void close() {
        s.close();
        ts.close();
        if(cs!=null)
            cs.close();
    }

    @Override
    public boolean hasField(String fldName) {
        return ts.hasField(fldName);
    }

    @Override
    public Constant getVal(String fldName) {
        Constant a=ts.getVal(fldName);
        // logger.info(tx.toString()+":"+ttblname+":"+a.toString());
        return a;
    }

    private void set_cs(){
        while(s.next())
        {
            // logger.info("set_cs:s.next()");
            int index = (int)s.getVal(IvfflatIndex.SCHEMA_ID).asJavaVal();
            ttblname = this.iia.indexName()+ "-"+ index ;
            // logger.info(tx.toString()+":s:"+ttblname);
			TableInfo tti = VanillaDb.catalogMgr().getTableInfo(ttblname, this.tx);
            if(tti==null)
            {
                // logger.info("set_cs:tti==null");
                continue;
            }
            else
            {
                // logger.info(tx.toString()+":set_cs:"+tti.toString());
                Plan ttp = new TablePlan(ttblname,this.tx);
                ttp = new SortPlan(ttp, this.indexDistanceFn, this.tx);
                if(cs!=null)
                    cs.close();
                cs = ttp.open();
                cs.beforeFirst();
                int con=0;
                while(cs.next())
                    con++;
                // logger.info(ttblname+":con:"+con);
                cs.beforeFirst();
                return;
            }
        }
        cs = null;
        // logger.info(tx.toString()+"set_cs:cs==null");
    }
}
