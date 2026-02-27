/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "bolt/dwio/common/Options.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

namespace bytedance::bolt::exec::test {

/// Contains the query plan and input data files keyed on source plan node ID.
/// All data files use the same file format specified in 'dataFileFormat'.
struct TpcdsPlan {
  core::PlanNodePtr plan;
  std::unordered_map<core::PlanNodeId, std::vector<std::string>> dataFiles;
  dwio::common::FileFormat dataFileFormat;
  std::string planName;
};

/// Contains type information, data files, and file column names for a table.
struct TpcdsTableMetadata {
  RowTypePtr type;
  std::vector<std::string> dataFiles;
  std::unordered_map<std::string, std::string> fileColumnNames;
};

/// Builds TPC-DS queries using TPC-DS data files located in the specified
/// directory. Each table data must be placed in hive-style partitioning. That
/// is, the top-level directory is expected to contain a sub-directory per table
/// name and the name of the sub-directory must match the table name. Example:
/// ls -R data/
///  store_sales   date_dim   item
///
///  data/store_sales:
///  store_sales.parquet
///
///  data/date_dim:
///  date_dim.parquet
class TpcdsQueryBuilder {
 public:
  explicit TpcdsQueryBuilder(
      dwio::common::FileFormat format,
      bool filtersAsNode = false)
      : format_(format), filtersAsNode_(filtersAsNode) {}

  /// Read each data file, initialize row types, and determine data paths for
  /// each table.
  void initialize(const std::string& dataPath);

  /// Get the query plan for a given TPC-DS query number.
  TpcdsPlan getQueryPlan(int queryId) const;

  /// Get the TPC-DS table names present.
  static const std::vector<std::string>& getTableNames();

 private:
  void readFileSchema(
      const std::string& tableName,
      const std::string& filePath,
      const std::vector<std::string>& columns);

  // TPC-DS query plans
  TpcdsPlan getQ1Plan() const;
  TpcdsPlan getQ3Plan() const;
  TpcdsPlan getQ10Plan() const;
  TpcdsPlan getQ30Plan() const;
  TpcdsPlan getQ63Plan() const;
  TpcdsPlan getQ89Plan() const;

  const std::vector<std::string>& getTableFilePaths(
      const std::string& tableName) const {
    return tableMetadata_.at(tableName).dataFiles;
  }

  std::shared_ptr<const RowType> getRowType(
      const std::string& tableName,
      const std::vector<std::string>& columnNames) const {
    BOLT_CHECK(tableMetadata_.size() > 0, "tableMetadata_ is empty");
    auto columnSelector = std::make_shared<dwio::common::ColumnSelector>(
        tableMetadata_.at(tableName).type, columnNames);
    return columnSelector->buildSelectedReordered();
  }

  const std::unordered_map<std::string, std::string>& getFileColumnNames(
      const std::string& tableName) const {
    return tableMetadata_.at(tableName).fileColumnNames;
  }

  std::unordered_map<std::string, TpcdsTableMetadata> tableMetadata_;
  const dwio::common::FileFormat format_;
  static const std::unordered_map<std::string, std::vector<std::string>>
      kTables_;
  static const std::vector<std::string> kTableNames_;

  // Fact tables
  static constexpr const char* kStoreSales = "store_sales";
  static constexpr const char* kStoreReturns = "store_returns";
  static constexpr const char* kCatalogSales = "catalog_sales";
  static constexpr const char* kCatalogReturns = "catalog_returns";
  static constexpr const char* kWebSales = "web_sales";
  static constexpr const char* kWebReturns = "web_returns";
  static constexpr const char* kInventory = "inventory";

  // Dimension tables
  static constexpr const char* kCustomer = "customer";
  static constexpr const char* kCustomerAddress = "customer_address";
  static constexpr const char* kCustomerDemographics = "customer_demographics";
  static constexpr const char* kDateDim = "date_dim";
  static constexpr const char* kTimeDim = "time_dim";
  static constexpr const char* kItem = "item";
  static constexpr const char* kStore = "store";
  static constexpr const char* kPromotion = "promotion";
  static constexpr const char* kHouseholdDemographics = "household_demographics";
  static constexpr const char* kWarehouse = "warehouse";
  static constexpr const char* kShipMode = "ship_mode";
  static constexpr const char* kReason = "reason";
  static constexpr const char* kIncomeBand = "income_band";
  static constexpr const char* kCallCenter = "call_center";
  static constexpr const char* kCatalogPage = "catalog_page";
  static constexpr const char* kWebPage = "web_page";
  static constexpr const char* kWebSite = "web_site";

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
  const bool filtersAsNode_;
};

} // namespace bytedance::bolt::exec::test
