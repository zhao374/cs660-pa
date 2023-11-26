//
// Created by Ricardo 李 on 2023/11/26.
//

#include "db/IntAggregateData.h"
IntAggregateData::IntAggregateData() {
    sum = 0;
    min = INT_MAX;
    max = INT_MIN;
    count = 0;
}

std::vector<int> IntAggregateData::getData() {
    return this->data;
}

int IntAggregateData::getMin() {return min;}

int IntAggregateData::getCount() {return count;};

int IntAggregateData::getMax() {return max;}

int IntAggregateData::getSum() {return sum;}

void IntAggregateData::setSum(int value) {sum = value;}

void IntAggregateData::setMax(int value) {max = value;}

void IntAggregateData::setMin(int value) {min = value;}

void IntAggregateData::increaseCount() {count++;}