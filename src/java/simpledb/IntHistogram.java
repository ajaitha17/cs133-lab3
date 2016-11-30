package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    int buckets, min, max, count;
    int[] value_buckets;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * Note: if the number of buckets exceeds the number of distinct integers between min and max, 
     * some buckets may remain empty (don't create buckets with non-integer widths).
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = buckets;
        this.min = min;
        this.max = max;
        value_buckets = new int[buckets];
        count = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // System.out.println((max - min + 1) + " " + this.buckets);
    	count++;
        int b = (v - this.min)/((int) Math.ceil((double)(this.max - this.min + 1)/this.buckets));
        this.value_buckets[b]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	double selectivity = 0.0, temp;
        int b = (v - this.min)/((int) Math.ceil((double)(this.max - this.min + 1)/this.buckets)), i;
        if(v < this.min)
            b = -1;
        if(v > this.max)
            b = this.buckets;

        // System.out.println(b);

        if(op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE || op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.GREATER_THAN_OR_EQ || op == Predicate.Op.NOT_EQUALS) {
            if(b >= 0 && b < this.buckets) {
                selectivity = 1.0 * value_buckets[b]/this.count;
            }
            if(op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE) {
                return selectivity;
            }
            if(op == Predicate.Op.NOT_EQUALS) {
                return (1.0 - selectivity);
            }
        }

        if(op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.LESS_THAN) {
            if(b == -1)
                return 0.0;

            if(b == this.buckets)
                return 1.0;

            temp = v - (this.min + (b * ((this.max - this.min)/this.buckets)));
            if(this.count == 0 || temp == 0.0) {
                selectivity += 0.0;
            }
            else {
                selectivity += ((1.0 * this.value_buckets[b])/temp)/this.count;
            }
            // System.out.println(temp);
            for(i=0;i<b;i++) {
                if(this.count == 0) 
                    selectivity += 0.0;
                else
                    selectivity += (1.0 * this.value_buckets[i])/this.count;
            }
            return selectivity;
        }

        if(op == Predicate.Op.GREATER_THAN_OR_EQ || op == Predicate.Op.GREATER_THAN) {
            if(b == this.buckets)
                return 0.0;

            if(b == -1)
                return 1.0;

            temp = (this.min + ((b + 1) * ((this.max - this.min)/this.buckets))) - v;
            if(this.count == 0 || temp == 0.0) {
                selectivity += 0.0;
            }
            else {
                selectivity += ((1.0 * this.value_buckets[b])/temp)/this.count;
            }

            for(i=b+1;i<this.buckets;i++) {
                if(this.count == 0)
                    selectivity += 0.0;
                else
                    selectivity += (1.0 * this.value_buckets[i])/this.count;
            }
        }
        // System.out.println("Returning outside");
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It could be used to
     *     implement a more efficient optimization
     *
     * Not necessary for lab 3
     * */
    public double avgSelectivity()
    {
        return 0.5;
    }
    
    /**
     * (Optional) A String representation of the contents of this histogram
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
