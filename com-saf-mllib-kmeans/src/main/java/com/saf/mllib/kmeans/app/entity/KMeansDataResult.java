package com.saf.mllib.kmeans.app.entity;

import org.apache.spark.mllib.clustering.KMeansModel;

import java.io.Serializable;
import java.util.List;

public class KMeansDataResult implements Serializable {
    private KMeansModel kMeansModel;

    private int bestK = 0;
    private int bestMaxIterator = 0;
    private int bestRun = 0;

    private List<KMeansData> dataList;

    public KMeansDataResult() {
    }

    public KMeansDataResult(KMeansModel kMeansModel, int bestK, int bestMaxIterator, int bestRun, List<KMeansData> dataList) {
        this.kMeansModel = kMeansModel;
        this.bestK = bestK;
        this.bestMaxIterator = bestMaxIterator;
        this.bestRun = bestRun;
        this.dataList = dataList;
    }

    public KMeansDataResult(KMeansModel kMeansModel, int bestK, int bestMaxIterator, int bestRun) {
        this.kMeansModel = kMeansModel;
        this.bestK = bestK;
        this.bestMaxIterator = bestMaxIterator;
        this.bestRun = bestRun;
    }


    public KMeansModel getkMeansModel() {
        return kMeansModel;
    }

    public void setkMeansModel(KMeansModel kMeansModel) {
        this.kMeansModel = kMeansModel;
    }

    public int getBestK() {
        return bestK;
    }

    public void setBestK(int bestK) {
        this.bestK = bestK;
    }

    public int getBestMaxIterator() {
        return bestMaxIterator;
    }

    public void setBestMaxIterator(int bestMaxIterator) {
        this.bestMaxIterator = bestMaxIterator;
    }

    public int getBestRun() {
        return bestRun;
    }

    public void setBestRun(int bestRun) {
        this.bestRun = bestRun;
    }

    public List<KMeansData> getDataList() {
        return dataList;
    }

    public void setDataList(List<KMeansData> dataList) {
        this.dataList = dataList;
    }

    public static class KMeansData implements Serializable {
        private String submissionId;

        private String data;

        private long group;

        public KMeansData(String submissionId, String data, long group) {
            this.submissionId = submissionId;
            this.data = data;
            this.group = group;
        }

        public String getSubmissionId() {
            return submissionId;
        }

        public void setSubmissionId(String submissionId) {
            this.submissionId = submissionId;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public long getGroup() {
            return group;
        }

        public void setGroup(long group) {
            this.group = group;
        }
    }
}
