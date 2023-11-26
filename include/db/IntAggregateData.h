//
// Created by Ricardo 李 on 2023/11/26.
//

#ifndef CS660_INTAGGREGATEDATA_H
#define CS660_INTAGGREGATEDATA_H


#include <vector>

class IntAggregateData {
private:
public:
    int  sum = 0, min = INT_MAX, max = INT_MIN, count = 0;
    std::vector<int> data;



    IntAggregateData();
    std::vector<int> getData();
    int getSum();
    int getMin();
    int getMax();
    int getCount();

    void setSum(int value);
    void setMin(int value);
    void setMax(int value);
    void increaseCount();

};


#endif //CS660_INTAGGREGATEDATA_H
