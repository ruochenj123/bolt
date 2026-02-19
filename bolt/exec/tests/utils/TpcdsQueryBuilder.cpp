/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/exec/tests/utils/TpcdsQueryBuilder.h"

#include "bolt/common/base/Fs.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/dwio/common/ReaderFactory.h"

#include <fstream>

namespace bytedance::bolt::exec::test {

const std::string TpcdsQueryBuilder::kCatalogSales = "catalog_sales";

// TPC-DS catalog_sales table columns (34 columns)
// Schema based on TPC-DS specification
const std::unordered_map<std::string, std::vector<std::string>>
    TpcdsQueryBuilder::kTables_ = {
        {kCatalogSales,
         {
             "cs_sold_date_sk",
             "cs_sold_time_sk",
             "cs_ship_date_sk",
             "cs_bill_customer_sk",
             "cs_bill_cdemo_sk",
             "cs_bill_hdemo_sk",
             "cs_bill_addr_sk",
             "cs_ship_customer_sk",
             "cs_ship_cdemo_sk",
             "cs_ship_hdemo_sk",
             "cs_ship_addr_sk",
             "cs_call_center_sk",
             "cs_catalog_page_sk",
             "cs_ship_mode_sk",
             "cs_warehouse_sk",
             "cs_item_sk",
             "cs_promo_sk",
             "cs_order_number",
             "cs_quantity",
             "cs_wholesale_cost",
             "cs_list_price",
             "cs_sales_price",
             "cs_ext_discount_amt",
             "cs_ext_sales_price",
             "cs_ext_wholesale_cost",
             "cs_ext_list_price",
             "cs_ext_tax",
             "cs_coupon_amt",
             "cs_ext_ship_cost",
             "cs_net_paid",
             "cs_net_paid_inc_tax",
             "cs_net_paid_inc_ship",
             "cs_net_paid_inc_ship_tax",
             "cs_net_profit",
         }}};

const std::vector<std::string> TpcdsQueryBuilder::kTableNames_ = {kCatalogSales};

void TpcdsQueryBuilder::readFileSchema(
    const std::string& tableName,
    const std::string& filePath,
    const std::vector<std::string>& columns) {
  dwio::common::ReaderOptions readerOptions{pool_.get()};
  readerOptions.setFileFormat(format_);
  auto uniqueReadFile =
      filesystems::getFileSystem(filePath, nullptr)->openFileForRead(filePath);
  std::shared_ptr<ReadFile> readFile;
  readFile.reset(uniqueReadFile.release());
  auto input = std::make_unique<dwio::common::BufferedInput>(
      readFile, readerOptions.getMemoryPool());
  std::unique_ptr<dwio::common::Reader> reader =
      dwio::common::getReaderFactory(readerOptions.getFileFormat())
          ->createReader(std::move(input), readerOptions);
  const auto fileType = reader->rowType();
  const auto fileColumnNames = fileType->names();
  // There can be extra columns in the file towards the end.
  BOLT_CHECK_GE(fileColumnNames.size(), columns.size());
  std::unordered_map<std::string, std::string> fileColumnNamesMap(
      columns.size());
  std::transform(
      columns.begin(),
      columns.end(),
      fileColumnNames.begin(),
      std::inserter(fileColumnNamesMap, fileColumnNamesMap.begin()),
      [](std::string a, std::string b) { return std::make_pair(a, b); });
  auto columnNames = columns;
  auto types = fileType->children();
  types.resize(columnNames.size());
  tableMetadata_[tableName].type =
      std::make_shared<RowType>(std::move(columnNames), std::move(types));
  tableMetadata_[tableName].fileColumnNames = std::move(fileColumnNamesMap);
}

void TpcdsQueryBuilder::initialize(const std::string& dataPath) {
  for (const auto& [tableName, columns] : kTables_) {
    const fs::path tablePath{dataPath + "/" + tableName};
    std::error_code error;
    bool anyFound = false;
    for (auto const& dirEntry : fs::directory_iterator{
             tablePath, std::filesystem::directory_options(), error}) {
      if (!dirEntry.is_regular_file()) {
        continue;
      }
      // Ignore hidden files.
      if (dirEntry.path().filename().c_str()[0] == '.') {
        continue;
      }
      if (tableMetadata_[tableName].dataFiles.empty()) {
        anyFound = true;
        readFileSchema(tableName, dirEntry.path().string(), columns);
      }
      tableMetadata_[tableName].dataFiles.push_back(dirEntry.path());
    }
    if (!anyFound && error) {
      std::ifstream file(tablePath);
      std::string line;
      while (std::getline(file, line)) {
        if (tableMetadata_[tableName].dataFiles.empty()) {
          readFileSchema(tableName, line, columns);
        }
        tableMetadata_[tableName].dataFiles.push_back(line);
      }
    }
  }
}

const std::vector<std::string>& TpcdsQueryBuilder::getTableNames() {
  return kTableNames_;
}

RowTypePtr TpcdsQueryBuilder::getRowType(
    const std::string& tableName,
    const std::vector<std::string>& columnNames) const {
  const auto& tableType = tableMetadata_.at(tableName).type;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(columnNames.size());
  types.reserve(columnNames.size());

  for (const auto& columnName : columnNames) {
    names.push_back(columnName);
    types.push_back(tableType->findChild(columnName));
  }
  return std::make_shared<RowType>(std::move(names), std::move(types));
}

const std::unordered_map<std::string, std::string>&
TpcdsQueryBuilder::getFileColumnNames(const std::string& tableName) const {
  return tableMetadata_.at(tableName).fileColumnNames;
}

std::vector<std::string> TpcdsQueryBuilder::getTableFilePaths(
    const std::string& tableName) const {
  return tableMetadata_.at(tableName).dataFiles;
}

TpchPlan TpcdsQueryBuilder::getQueryPlan(int queryId) const {
  switch (queryId) {
    case 1:
      return getSortPlan1Key();
    case 2:
      return getSortPlan2Keys();
    case 3:
      return getSortPlan3Keys();
    case 4:
      return getSortPlan4Keys();
    default:
      BOLT_FAIL("TPC-DS sort query {} not supported", queryId);
  }
}

// Sort query 1: Sort by cs_warehouse_sk (1 key)
// SELECT * FROM catalog_sales ORDER BY cs_warehouse_sk
TpchPlan TpcdsQueryBuilder::getSortPlan1Key() const {
  const auto& columns = kTables_.at(kCatalogSales);
  const auto selectedRowType = getRowType(kCatalogSales, columns);
  const auto& fileColumnNames = getFileColumnNames(kCatalogSales);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanNodeId;

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .filtersAsNode(filtersAsNode_)
          .tableScan(kCatalogSales, selectedRowType, fileColumnNames)
          .captureScanNodeId(scanNodeId)
          .orderBy({"cs_warehouse_sk"}, false)
          .planNode();

  TpchPlan context;
  context.planName = "sort_1key";
  context.plan = std::move(plan);
  context.dataFiles[scanNodeId] = getTableFilePaths(kCatalogSales);
  context.dataFileFormat = format_;
  return context;
}

// Sort query 2: Sort by cs_warehouse_sk, cs_ship_mode_sk (2 keys)
TpchPlan TpcdsQueryBuilder::getSortPlan2Keys() const {
  const auto& columns = kTables_.at(kCatalogSales);
  const auto selectedRowType = getRowType(kCatalogSales, columns);
  const auto& fileColumnNames = getFileColumnNames(kCatalogSales);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanNodeId;

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .filtersAsNode(filtersAsNode_)
          .tableScan(kCatalogSales, selectedRowType, fileColumnNames)
          .captureScanNodeId(scanNodeId)
          .orderBy({"cs_warehouse_sk", "cs_ship_mode_sk"}, false)
          .planNode();

  TpchPlan context;
  context.planName = "sort_2keys";
  context.plan = std::move(plan);
  context.dataFiles[scanNodeId] = getTableFilePaths(kCatalogSales);
  context.dataFileFormat = format_;
  return context;
}

// Sort query 3: Sort by cs_warehouse_sk, cs_ship_mode_sk, cs_promo_sk (3 keys)
TpchPlan TpcdsQueryBuilder::getSortPlan3Keys() const {
  const auto& columns = kTables_.at(kCatalogSales);
  const auto selectedRowType = getRowType(kCatalogSales, columns);
  const auto& fileColumnNames = getFileColumnNames(kCatalogSales);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanNodeId;

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .filtersAsNode(filtersAsNode_)
          .tableScan(kCatalogSales, selectedRowType, fileColumnNames)
          .captureScanNodeId(scanNodeId)
          .orderBy({"cs_warehouse_sk", "cs_ship_mode_sk", "cs_promo_sk"}, false)
          .planNode();

  TpchPlan context;
  context.planName = "sort_3keys";
  context.plan = std::move(plan);
  context.dataFiles[scanNodeId] = getTableFilePaths(kCatalogSales);
  context.dataFileFormat = format_;
  return context;
}

// Sort query 4: Sort by cs_warehouse_sk, cs_ship_mode_sk, cs_promo_sk, cs_quantity (4 keys)
TpchPlan TpcdsQueryBuilder::getSortPlan4Keys() const {
  const auto& columns = kTables_.at(kCatalogSales);
  const auto selectedRowType = getRowType(kCatalogSales, columns);
  const auto& fileColumnNames = getFileColumnNames(kCatalogSales);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanNodeId;

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .filtersAsNode(filtersAsNode_)
          .tableScan(kCatalogSales, selectedRowType, fileColumnNames)
          .captureScanNodeId(scanNodeId)
          .orderBy(
              {"cs_warehouse_sk", "cs_ship_mode_sk", "cs_promo_sk", "cs_quantity"},
              false)
          .planNode();

  TpchPlan context;
  context.planName = "sort_4keys";
  context.plan = std::move(plan);
  context.dataFiles[scanNodeId] = getTableFilePaths(kCatalogSales);
  context.dataFileFormat = format_;
  return context;
}

} // namespace bytedance::bolt::exec::test
