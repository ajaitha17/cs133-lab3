package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    int tableid, ioCostPerPage, count;
    DbFile dbf;
    HashMap<Integer, Integer> max;
    HashMap<Integer, Integer> min;
    HashMap<Integer, IntHistogram> inthistogram;
    HashMap<Integer, StringHistogram> stringhistogram;
    TupleDesc td;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
	// See project description for hint on using a Transaction
	
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.count = 0;
        dbf = Database.getCatalog().getDatabaseFile(this.tableid);
        max = new HashMap<Integer, Integer>();
        min = new HashMap<Integer, Integer>();
        inthistogram = new HashMap<Integer, IntHistogram>();
        stringhistogram = new HashMap<Integer, StringHistogram>();
        td = Database.getCatalog().getTupleDesc(this.tableid);
        this.setMinMax();
        
        TupleDesc td = Database.getCatalog().getTupleDesc(this.tableid);
        int i,numFields = td.numFields();
        // System.out.println(this.min.toString());
        // System.out.println(this.max.toString());
        // System.out.println(this.min.get(0));
        // System.out.println(this.max.get(0));

        Type type;
        // System.out.println(td.toString());

        for(i=0;i<numFields;i++) {
            type = td.getFieldType(i);
            if(type.equals(Type.INT_TYPE)) {
                // System.out.println(min.get(i));
                // System.out.println(max.get(i));
                IntHistogram ih;
                try{
                    ih = new IntHistogram(NUM_HIST_BINS, this.min.get(i), this.max.get(i));    
                }
                catch(NullPointerException e) {
                    ih = new IntHistogram(NUM_HIST_BINS, Integer.MAX_VALUE, Integer.MIN_VALUE);    
                }
                inthistogram.put(i, ih);
            }
            else {
                StringHistogram sh = new StringHistogram(NUM_HIST_BINS);
                stringhistogram.put(i, sh);
            }
        }

        this.setHistograms();
    }



    public void setMinMax() {
        TransactionId tid = new TransactionId();
        SeqScan s = new SeqScan(tid, tableid, "Alias");
        // DbFileIterator it = dbf.iterator(tid);
        int numFields = td.numFields(), i;
        Type type;
        int curr;
        try 
        {
            s.open();
            while(s.hasNext()) {
                Tuple tuple = s.next();
                count++;
                for(i=0;i<numFields;i++) {
                    type = td.getFieldType(i);
                    if(type.equals(Type.INT_TYPE)) {
                        curr = ((IntField) tuple.getField(i)).getValue();
                        if(min.containsKey(i)) {
                            if(min.get(i) > curr) {
                                min.put(i, curr);
                            }
                            if(max.get(i) < curr) {
                                max.put(i, curr);
                            }
                        }
                        else {
                            min.put(i, curr);
                            max.put(i, curr);
                        }
                    }
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void setHistograms() {
        TransactionId tid = new TransactionId();
        SeqScan it = new SeqScan(tid, this.tableid, "Alias");
        // DbFileIterator it = dbf.iterator(tid);
        TupleDesc td = Database.getCatalog().getTupleDesc(this.tableid);
        int numFields = td.numFields(), i;
        Tuple tuple;
        Type type;
        int ifield;
        String sfield;
        try {
            it.open();
            while(it.hasNext()) {
                tuple = it.next();
                for(i=0;i<numFields;i++) {
                    type = td.getFieldType(i);
                    if(type.equals(Type.INT_TYPE)) {
                        ifield = ((IntField) tuple.getField(i)).getValue();
                        this.inthistogram.get(i).addValue(ifield);
                    }
                    else {
                        sfield = ((StringField) tuple.getField(i)).getValue();
                        this.stringhistogram.get(i).addValue(sfield);
                    }
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return ((HeapFile)this.dbf).numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (selectivityFactor * this.count);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     *
     * Not necessary for lab 3
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 0.5;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        TupleDesc td = Database.getCatalog().getTupleDesc(this.tableid);
        // System.out.println("Table Stats estimateSelectivity --- " + td.toString());
        Type type = td.getFieldType(field);
        if(type.equals(Type.INT_TYPE)) {
            int c = ((IntField) constant).getValue();
            IntHistogram ih = this.inthistogram.get(field);
            return ih.estimateSelectivity(op, c);
        }
        else {
            String c = ((StringField) constant).getValue();
            StringHistogram sh = this.stringhistogram.get(field);
            return sh.estimateSelectivity(op, c);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.count;
    }

}
