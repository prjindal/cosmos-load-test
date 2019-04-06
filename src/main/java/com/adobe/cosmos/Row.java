package com.adobe.cosmos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Row {

  public static final String PARTITION_KEY = "prtId";
  public static final String SORT_KEY = "pnsn";

  private static final String UNDERSCORE = "_";

  @JsonProperty("cId")
  private String childId;

  @JsonProperty(PARTITION_KEY)
  private String partitionId;

  @JsonProperty("csn")
  private Long childSequenceNumber;

  @JsonProperty("posn")
  private Long parentOldSeqNumber;

  @JsonProperty(SORT_KEY)
  private Long parentNewSeqNumber;

  //time at which event was persisted
  @JsonProperty("t")
  private Long timestamp;

  @JsonProperty("pyld")
  private String payload;

  @JsonProperty("idScp")
  private String idScope;

  // operation type : MOVE, CREATE, DELETE
  @JsonProperty("opTyp")
  private String operationType;

  // child asset type : FILE, DIRECTORY, ENTITY
  @JsonProperty("asTyp")
  private String assetType;

  //applicable in case of move
  @JsonProperty("nPId")
  private String newParentId;

  @JsonProperty("nPOSeqNo")
  private Long newParentOldSequenceNumber;

  @JsonProperty("nPNSeqNo")
  private Long newParentNewSequenceNumber;

  public String getChildId() {
    return childId;
  }

  public void setChildId(final String childId) {
    this.childId = childId;
  }

  public String getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(final String partitionId) {
    this.partitionId = partitionId;
  }

  public Long getChildSequenceNumber() {
    return childSequenceNumber;
  }

  public void setChildSequenceNumber(final Long childSequenceNumber) {
    this.childSequenceNumber = childSequenceNumber;
  }

  public Long getParentOldSeqNumber() {
    return parentOldSeqNumber;
  }

  public void setParentOldSeqNumber(final Long parentOldSeqNumber) {
    this.parentOldSeqNumber = parentOldSeqNumber;
  }

  public Long getParentNewSeqNumber() {
    return parentNewSeqNumber;
  }

  public void setParentNewSeqNumber(final Long parentNewSeqNumber) {
    this.parentNewSeqNumber = parentNewSeqNumber;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(final Long timestamp) {
    this.timestamp = timestamp;
  }

  public String getPayload() {
    return payload;
  }

  public void setPayload(final String payload) {
    this.payload = payload;
  }

  public String getIdScope() {
    return idScope;
  }

  public void setIdScope(final String idScope) {
    this.idScope = idScope;
  }

  public String getOperationType() {
    return operationType;
  }

  public void setOperationType(final String operationType) {
    this.operationType = operationType;
  }

  public void setAssetType(final String assetType) {
    this.assetType = assetType;
  }

  public String getAssetType() {
    return assetType;
  }

  public String getNewParentId() {
    return newParentId;
  }

  public void setNewParentId(final String newParentId) {
    this.newParentId = newParentId;
  }

  public Long getNewParentOldSequenceNumber() {
    return newParentOldSequenceNumber;
  }

  public void setNewParentOldSequenceNumber(final Long newParentOldSequenceNumber) {
    this.newParentOldSequenceNumber = newParentOldSequenceNumber;
  }

  public Long getNewParentNewSequenceNumber() {
    return newParentNewSequenceNumber;
  }

  public void setNewParentNewSequenceNumber(final Long newParentNewSequenceNumber) {
    this.newParentNewSequenceNumber = newParentNewSequenceNumber;
  }
}
