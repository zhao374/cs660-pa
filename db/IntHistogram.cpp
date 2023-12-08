#include <db/IntHistogram.h>
#include <vector>
#include <cmath>
#include <db/Predicate.h>
#include <sstream>

using namespace db;
IntHistogram::IntHistogram(int buckets, int min, int max) {
    this->buckets=std::vector<int>(buckets);
    this->max=max;
    this->min=min;
    this->numBuckets=buckets;
    this->ntups=0;
    this->width = (int) ceil((max - min + 1) / (double) buckets);
}

void IntHistogram::addValue(int v) {
    int index = (v - min) / width;
    if (index < numBuckets) {
        buckets[index]++;
        ntups++;
    }
    // TODO pa4.1: some code goes here
}

double IntHistogram::estimateSelectivity(Predicate::Op op, int v) const {
    // the pivot
    int index = (v - min) / width;

    // error input
    if (index < 0 || index >= numBuckets) {
        // If the value is out of the histogram's range, return 0 or 1 based on the operation
        if (op ==Predicate::Op::LESS_THAN ||op==Predicate::Op::LESS_THAN_OR_EQ) {
            return 1.0;
        } else if (op == Predicate::Op::GREATER_THAN || op == Predicate::Op::GREATER_THAN_OR_EQ) {
            return 0.0;
        }
        return 0.0;
    }

    double selectivity = 0.0;
    switch (op) {
        case Predicate::Op::EQUALS:
            selectivity = (buckets[index] / (double) width) / ntups;
            break;
        case Predicate::Op::GREATER_THAN:
            if (v < min) return 1;
            if (v > max) return 0;
            // Calculate selectivity for the bucket containing the value and all buckets to the right
            for (int i = v + 1; i <= max; i++) {
                index = (i - min)/width;
                selectivity += (buckets[index] / (double) width) / ntups;
            }
            break;
        case Predicate::Op::GREATER_THAN_OR_EQ:
            if (v < min) return 1;
            if (v > max) return 0;
            for (int i = v; i <= max; i++) {
                index = (i - min)/width;
                selectivity += (buckets[index] / (double) width) / ntups;
            }
            break;
        case Predicate::Op::LESS_THAN:
            if (v < min) return 0;
            if (v > max) return 1;
            for (int i = v -1; i >= min; i--) {
                index = (i-min)/width;
                selectivity += (buckets[index] / (double) width) / ntups;
            }
            break;
        case Predicate::Op::LESS_THAN_OR_EQ:
            if (v < min) return 1;
            if (v > max) return 0;
            for (int i = v; i >= min; i--) {
                index = (i-min)/width;
                selectivity += (buckets[index] / (double) width) / ntups;
            }
            break;

        case Predicate::Op::NOT_EQUALS:
            if (index >= 0 && index < numBuckets) {
                selectivity = 1.0 - (buckets[index] / (double) width) / ntups;
            }
            break;
    }
    return selectivity;
    // TODO pa4.1: some code goes here
}

double IntHistogram::avgSelectivity() const {
    double totalSelectivity = 0.0;
    for (int bucket : buckets) {
        totalSelectivity += (bucket / (double) width) / ntups;
    }
    return totalSelectivity / numBuckets;
    // TODO pa4.1: some code goes here
}

std::string IntHistogram::to_string() const {
    std::ostringstream sb;
    sb << "IntHistogram(";
    for (int i = 0; i < numBuckets; i++) {
        sb << "Bucket " << i << ": " << buckets[i] << ", ";
    }

    std::string str = sb.str();
    str = str.substr(0, str.length() - 2); // Remove trailing comma and space
    str += ")";
    return str;
    // TODO pa4.1: some code goes here
}
