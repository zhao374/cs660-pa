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
    int index = (v - min) / width;
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
            // Calculate selectivity for the bucket containing the value and all buckets to the right
            for (int i = index + 1; i < numBuckets; i++) {
                selectivity += buckets[i];
            }
            selectivity += (buckets[index] / (double) width) * (width - (v % width));
            selectivity /= ntups;
            break;
        case Predicate::Op::LESS_THAN:
            for (int i = 0; i < index; i++) {
                selectivity += buckets[i];
            }
            if (index >= 0 && index < numBuckets) {
                selectivity += (buckets[index] / (double) width) * ((v % width) / (double) width);
            }
            selectivity /= ntups;
            break;

        case Predicate::Op::LESS_THAN_OR_EQ:
            for (int i = 0; i <= index; i++) {
                selectivity += buckets[i];
            }
            selectivity /= ntups;
            break;

        case Predicate::Op::GREATER_THAN_OR_EQ:
            for (int i = index; i < numBuckets; i++) {
                selectivity += buckets[i];
            }
            selectivity /= ntups;
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
