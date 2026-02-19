/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "bolt/dwio/common/Options.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TpchQueryBuilder.h"  // Reuse TpchPlan

namespace bytedance::bolt::exec::test {

// Reuse TpchPlan for TPC-DS since it has the same structure.
using TpcdsPlan = TpchPlan;

/// Contains type information, data files, and file column names for a table.
struct TpcdsTableMetadata {
  RowTypePtr type;
  std::vector<std::string> dataFiles;
  std::unordered_map<std::string, std::string> fileColumnNames;
};

/// Builds TPC-DS sort queries for benchmarking.
/// Currently supports catalog_sales table with 1-4 sort key variations.
class TpcdsQueryBuilder {
 public:
  explicit TpcdsQueryBuilder(
      dwio::common::FileFormat format,
      bool filtersAsNode = false)
      : format_(format), filtersAsNode_(filtersAsNode) {}

  /// Initialize from data path (expects Hive-style partitioning)
  void initialize(const std::string& dataPath);

  /// Get the query plan for a given query number.
  /// Query 1: Sort by cs_warehouse_sk (1 key)
  /// Query 2: Sort by cs_warehouse_sk, cs_ship_mode_sk (2 keys)
  /// Query 3: Sort by cs_warehouse_sk, cs_ship_mode_sk, cs_promo_sk (3 keys)
  /// Query 4: Sort by cs_warehouse_sk, cs_ship_mode_sk, cs_promo_sk, cs_quantity (4 keys)
  TpchPlan getQueryPlan(int queryId) const;

  /// Get the TPC-DS table names present.
  static const std::vector<std::string>& getTableNames();

 private:
  static const std::string kCatalogSales;

  // catalog_sales columns in TPC-DS order
  static const std::unordered_map<std::string, std::vector<std::string>>
      kTables_;
  static const std::vector<std::string> kTableNames_;

  void readFileSchema(
      const std::string& tableName,
      const std::string& filePath,
      const std::vector<std::string>& columns);

  RowTypePtr getRowType(
      const std::string& tableName,
      const std::vector<std::string>& columnNames) const;

  const std::unordered_map<std::string, std::string>& getFileColumnNames(
      const std::string& tableName) const;

  std::vector<std::string> getTableFilePaths(
      const std::string& tableName) const;

  // Sort query plans with varying number of keys
  TpchPlan getSortPlan1Key() const;   // cs_warehouse_sk
  TpchPlan getSortPlan2Keys() const;  // + cs_ship_mode_sk
  TpchPlan getSortPlan3Keys() const;  // + cs_promo_sk
  TpchPlan getSortPlan4Keys() const;  // + cs_quantity

  dwio::common::FileFormat format_;
  bool filtersAsNode_ = false;
  std::unordered_map<std::string, TpcdsTableMetadata> tableMetadata_;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::deprecatedAddDefaultLeafMemoryPool()};
};

} // namespace bytedance::bolt::exec::test
