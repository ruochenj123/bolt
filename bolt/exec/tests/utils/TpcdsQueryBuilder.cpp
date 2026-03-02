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

TpcdsPlan TpcdsQueryBuilder::getQueryPlan(int queryId) const {
  switch (queryId) {
    case 1:
      return getQ1Plan();
    case 3:
      return getQ3Plan();
    case 7:
      return getQ7Plan();
    case 10:
      return getQ10Plan();
    case 13:
      return getQ13Plan();
    case 15:
      return getQ15Plan();
    case 18:
      return getQ18Plan();
    case 19:
      return getQ19Plan();
    case 25:
      return getQ25Plan();
    case 26:
      return getQ26Plan();
    case 29:
      return getQ29Plan();
    case 30:
      return getQ30Plan();
    case 42:
      return getQ42Plan();
    case 43:
      return getQ43Plan();
    case 46:
      return getQ46Plan();
    case 52:
      return getQ52Plan();
    case 53:
      return getQ53Plan();
    case 55:
      return getQ55Plan();
    case 63:
      return getQ63Plan();
    case 68:
      return getQ68Plan();
    case 73:
      return getQ73Plan();
    case 79:
      return getQ79Plan();
    case 89:
      return getQ89Plan();
    case 96:
      return getQ96Plan();
    default:
      BOLT_UNSUPPORTED("TPC-DS query {} is not supported", queryId);
  }
}

// Simple Q1: Just scan the small 'reason' table (only 3 columns, ~55 rows)
// SELECT * FROM reason
TpcdsPlan TpcdsQueryBuilder::getQ1Plan() const {
  std::vector<std::string> reasonCols = {"r_reason_sk", "r_reason_id", "r_reason_desc"};
  auto reasonType = getRowType(kReason, reasonCols);
  const auto& reasonFileColumnNames = getFileColumnNames(kReason);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId reasonScanId;

  auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(kReason, reasonType, reasonFileColumnNames, {})
                  .capturePlanNodeId(reasonScanId)
                  .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[reasonScanId] = getTableFilePaths(kReason);
  result.dataFileFormat = format_;
  result.planName = "Q1";
  return result;
}

// Q3: 3-way join date_dim, store_sales, item with aggregation and sort
// SELECT d_year, i_brand_id, i_brand, sum(ss_ext_sales_price)
// FROM date_dim, store_sales, item
// WHERE d_date_sk = ss_sold_date_sk AND ss_item_sk = i_item_sk
//   AND i_manufact_id = 128 AND d_moy = 11
// GROUP BY d_year, i_brand, i_brand_id
// ORDER BY d_year, sum_agg DESC, i_brand_id
// LIMIT 100
TpcdsPlan TpcdsQueryBuilder::getQ3Plan() const {
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_moy"};
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"};
  std::vector<std::string> itemCols = {
      "i_item_sk", "i_brand_id", "i_brand", "i_manufact_id"};

  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto itemType = getRowType(kItem, itemCols);

  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId dateDimScanId;
  core::PlanNodeId storeSalesScanId;
  core::PlanNodeId itemScanId;

  // Build date_dim scan with filter d_moy = 11
  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_moy = 11"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  // Build item scan with filter i_manufact_id = 128
  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {"i_manufact_id = 128"})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  // store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
  // then JOIN item ON ss_item_sk = i_item_sk
  // then aggregate and sort
  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin(
              {"ss_sold_date_sk"},
              {"d_date_sk"},
              dateDimNode,
              "",
              {"ss_item_sk", "ss_ext_sales_price", "d_year"})
          .hashJoin(
              {"ss_item_sk"},
              {"i_item_sk"},
              itemNode,
              "",
              {"d_year", "i_brand_id", "i_brand", "ss_ext_sales_price"})
          .partialAggregation(
              {"d_year", "i_brand_id", "i_brand"},
              {"sum(ss_ext_sales_price) as sum_agg"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"d_year", "sum_agg DESC", "i_brand_id"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFileFormat = format_;
  result.planName = "Q3";
  return result;
}

// Q10: customer demographics aggregation (simplified)
// SELECT cd_gender, cd_marital_status, cd_education_status,
//        count(*), cd_purchase_estimate, cd_credit_rating,
//        cd_dep_count, cd_dep_employed_count, cd_dep_college_count
// FROM customer c, customer_address ca, customer_demographics cd,
//      store_sales ss, date_dim d
// WHERE c.c_current_addr_sk = ca.ca_address_sk
//   AND cd.cd_demo_sk = c.c_current_cdemo_sk
//   AND c.c_customer_sk = ss.ss_customer_sk
//   AND ss.ss_sold_date_sk = d.d_date_sk
//   AND d.d_year = 2002 AND d.d_moy BETWEEN 1 AND 4
// GROUP BY cd_gender, cd_marital_status, cd_education_status, ...
// ORDER BY cd_gender, cd_marital_status, ...
TpcdsPlan TpcdsQueryBuilder::getQ10Plan() const {
  std::vector<std::string> customerCols = {
      "c_customer_sk", "c_current_addr_sk", "c_current_cdemo_sk"};
  std::vector<std::string> customerAddrCols = {"ca_address_sk", "ca_county"};
  std::vector<std::string> customerDemoCols = {
      "cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status",
      "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count",
      "cd_dep_employed_count", "cd_dep_college_count"};
  std::vector<std::string> storeSalesCols = {
      "ss_customer_sk", "ss_sold_date_sk"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_moy"};

  auto customerType = getRowType(kCustomer, customerCols);
  auto customerAddrType = getRowType(kCustomerAddress, customerAddrCols);
  auto customerDemoType = getRowType(kCustomerDemographics, customerDemoCols);
  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);

  const auto& custFileColumnNames = getFileColumnNames(kCustomer);
  const auto& custAddrFileColumnNames = getFileColumnNames(kCustomerAddress);
  const auto& custDemoFileColumnNames = getFileColumnNames(kCustomerDemographics);
  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanId;
  core::PlanNodeId customerAddrScanId;
  core::PlanNodeId customerDemoScanId;
  core::PlanNodeId storeSalesScanId;
  core::PlanNodeId dateDimScanId;

  // date_dim with filter: d_year = 2002, d_moy between 1 and 4
  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year = 2002", "d_moy between 1 and 4"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  // customer_address
  auto custAddrNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomerAddress, customerAddrType, custAddrFileColumnNames, {})
                          .capturePlanNodeId(customerAddrScanId)
                          .planNode();

  // customer_demographics
  auto custDemoNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomerDemographics, customerDemoType, custDemoFileColumnNames, {})
                          .capturePlanNodeId(customerDemoScanId)
                          .planNode();

  // customer
  auto customerNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomer, customerType, custFileColumnNames, {})
                          .capturePlanNodeId(customerScanId)
                          .planNode();

  // Build the join chain:
  // store_sales -> JOIN date_dim -> JOIN customer -> JOIN customer_address -> JOIN customer_demographics
  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin(
              {"ss_sold_date_sk"},
              {"d_date_sk"},
              dateDimNode,
              "",
              {"ss_customer_sk"})
          .hashJoin(
              {"ss_customer_sk"},
              {"c_customer_sk"},
              customerNode,
              "",
              {"c_current_addr_sk", "c_current_cdemo_sk"})
          .hashJoin(
              {"c_current_addr_sk"},
              {"ca_address_sk"},
              custAddrNode,
              "",
              {"c_current_cdemo_sk"})
          .hashJoin(
              {"c_current_cdemo_sk"},
              {"cd_demo_sk"},
              custDemoNode,
              "",
              {"cd_gender", "cd_marital_status", "cd_education_status",
               "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count",
               "cd_dep_employed_count", "cd_dep_college_count"})
          .partialAggregation(
              {"cd_gender", "cd_marital_status", "cd_education_status",
               "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count",
               "cd_dep_employed_count", "cd_dep_college_count"},
              {"count(1) as cnt"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"cd_gender", "cd_marital_status", "cd_education_status",
                    "cd_purchase_estimate", "cd_credit_rating"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[customerScanId] = getTableFilePaths(kCustomer);
  result.dataFiles[customerAddrScanId] = getTableFilePaths(kCustomerAddress);
  result.dataFiles[customerDemoScanId] = getTableFilePaths(kCustomerDemographics);
  result.dataFileFormat = format_;
  result.planName = "Q10";
  return result;
}

// Q18: Item analysis with many payload columns (ideal for hybrid join testing)
// SELECT i_item_id, i_item_desc, i_current_price, i_wholesale_cost,
//        i_brand, i_class, i_category, i_manufact, i_size, i_color,
//        cd_gender, cd_marital_status, cd_education_status,
//        count(*) as cnt,
//        avg(cs_quantity) as agg1, avg(cs_list_price) as agg2
// FROM catalog_sales, item, date_dim, customer_demographics
// WHERE cs_sold_date_sk = d_date_sk
//   AND cs_item_sk = i_item_sk
//   AND cs_bill_cdemo_sk = cd_demo_sk
//   AND d_year = 2001
//   AND cd_gender = 'M'
//   AND cd_education_status = 'College'
// GROUP BY i_item_id, i_item_desc, i_current_price, i_wholesale_cost,
//          i_brand, i_class, i_category, i_manufact, i_size, i_color,
//          cd_gender, cd_marital_status, cd_education_status
// ORDER BY cnt DESC
// LIMIT 100
TpcdsPlan TpcdsQueryBuilder::getQ18Plan() const {
  // Item table with MANY payload columns (ideal for hybrid join)
  std::vector<std::string> itemCols = {
      "i_item_sk", "i_item_id", "i_item_desc", "i_current_price",
      "i_wholesale_cost", "i_brand", "i_class", "i_category",
      "i_manufact", "i_size", "i_color"};
  std::vector<std::string> catalogSalesCols = {
      "cs_sold_date_sk", "cs_item_sk", "cs_bill_cdemo_sk",
      "cs_quantity", "cs_list_price"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year"};
  std::vector<std::string> customerDemoCols = {
      "cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"};

  auto itemType = getRowType(kItem, itemCols);
  auto catalogSalesType = getRowType(kCatalogSales, catalogSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto customerDemoType = getRowType(kCustomerDemographics, customerDemoCols);

  const auto& itemFileColumnNames = getFileColumnNames(kItem);
  const auto& csFileColumnNames = getFileColumnNames(kCatalogSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& cdFileColumnNames = getFileColumnNames(kCustomerDemographics);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId itemScanId;
  core::PlanNodeId catalogSalesScanId;
  core::PlanNodeId dateDimScanId;
  core::PlanNodeId customerDemoScanId;

  // date_dim with filter: d_year = 2001
  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year = 2001"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  // item - NO filter, many payload columns
  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  // customer_demographics with filter: cd_gender = 'M', cd_education_status = 'College'
  auto custDemoNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomerDemographics, customerDemoType,
                                     cdFileColumnNames,
                                     {"cd_gender = 'M'", "cd_education_status = 'College'"})
                          .capturePlanNodeId(customerDemoScanId)
                          .planNode();

  // Build the join chain:
  // catalog_sales -> JOIN date_dim -> JOIN item -> JOIN customer_demographics
  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kCatalogSales, catalogSalesType, csFileColumnNames, {})
          .capturePlanNodeId(catalogSalesScanId)
          .hashJoin(
              {"cs_sold_date_sk"},
              {"d_date_sk"},
              dateDimNode,
              "",
              {"cs_item_sk", "cs_bill_cdemo_sk", "cs_quantity", "cs_list_price"})
          .hashJoin(
              {"cs_item_sk"},
              {"i_item_sk"},
              itemNode,
              "",
              {"cs_bill_cdemo_sk", "cs_quantity", "cs_list_price",
               "i_item_id", "i_item_desc", "i_current_price", "i_wholesale_cost",
               "i_brand", "i_class", "i_category", "i_manufact", "i_size", "i_color"})
          .hashJoin(
              {"cs_bill_cdemo_sk"},
              {"cd_demo_sk"},
              custDemoNode,
              "",
              {"cs_quantity", "cs_list_price",
               "i_item_id", "i_item_desc", "i_current_price", "i_wholesale_cost",
               "i_brand", "i_class", "i_category", "i_manufact", "i_size", "i_color",
               "cd_gender", "cd_marital_status", "cd_education_status"})
          .partialAggregation(
              {"i_item_id", "i_item_desc", "i_current_price", "i_wholesale_cost",
               "i_brand", "i_class", "i_category", "i_manufact", "i_size", "i_color",
               "cd_gender", "cd_marital_status", "cd_education_status"},
              {"count(1) as cnt", "avg(cs_quantity) as agg1", "avg(cs_list_price) as agg2"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"cnt DESC"}, false)
          // LIMIT 100 removed for benchmarking
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[catalogSalesScanId] = getTableFilePaths(kCatalogSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFiles[customerDemoScanId] = getTableFilePaths(kCustomerDemographics);
  result.dataFileFormat = format_;
  result.planName = "Q18";
  return result;
}

// Q30: web_returns with customer (simplified without CTE and correlated subquery)
// SELECT c_customer_id, c_salutation, c_first_name, c_last_name,
//        sum(wr_return_amt) as total_return
// FROM web_returns, date_dim, customer_address, customer
// WHERE wr_returned_date_sk = d_date_sk AND d_year = 2002
//   AND wr_returning_addr_sk = ca_address_sk
//   AND ca_state = 'GA'
//   AND wr_returning_customer_sk = c_customer_sk
// GROUP BY c_customer_id, c_salutation, c_first_name, c_last_name
// ORDER BY c_customer_id
// LIMIT 100
TpcdsPlan TpcdsQueryBuilder::getQ30Plan() const {
  std::vector<std::string> webReturnsCols = {
      "wr_returned_date_sk", "wr_returning_customer_sk",
      "wr_returning_addr_sk", "wr_return_amt"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year"};
  std::vector<std::string> customerAddrCols = {"ca_address_sk", "ca_state"};
  std::vector<std::string> customerCols = {
      "c_customer_sk", "c_customer_id", "c_salutation",
      "c_first_name", "c_last_name"};

  auto webReturnsType = getRowType(kWebReturns, webReturnsCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto customerAddrType = getRowType(kCustomerAddress, customerAddrCols);
  auto customerType = getRowType(kCustomer, customerCols);

  const auto& wrFileColumnNames = getFileColumnNames(kWebReturns);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& caFileColumnNames = getFileColumnNames(kCustomerAddress);
  const auto& custFileColumnNames = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId webReturnsScanId;
  core::PlanNodeId dateDimScanId;
  core::PlanNodeId customerAddrScanId;
  core::PlanNodeId customerScanId;

  // date_dim with filter d_year = 2002
  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year = 2002"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  // customer_address with filter ca_state = 'GA'
  auto custAddrNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomerAddress, customerAddrType, caFileColumnNames, {"ca_state = 'GA'"})
                          .capturePlanNodeId(customerAddrScanId)
                          .planNode();

  // customer
  auto customerNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomer, customerType, custFileColumnNames, {})
                          .capturePlanNodeId(customerScanId)
                          .planNode();

  // Build join chain:
  // web_returns -> JOIN date_dim -> JOIN customer_address -> JOIN customer -> aggregate -> sort
  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kWebReturns, webReturnsType, wrFileColumnNames, {})
          .capturePlanNodeId(webReturnsScanId)
          .hashJoin(
              {"wr_returned_date_sk"},
              {"d_date_sk"},
              dateDimNode,
              "",
              {"wr_returning_customer_sk", "wr_returning_addr_sk", "wr_return_amt"})
          .hashJoin(
              {"wr_returning_addr_sk"},
              {"ca_address_sk"},
              custAddrNode,
              "",
              {"wr_returning_customer_sk", "wr_return_amt"})
          .hashJoin(
              {"wr_returning_customer_sk"},
              {"c_customer_sk"},
              customerNode,
              "",
              {"c_customer_id", "c_salutation", "c_first_name", "c_last_name", "wr_return_amt"})
          .partialAggregation(
              {"c_customer_id", "c_salutation", "c_first_name", "c_last_name"},
              {"sum(wr_return_amt) as total_return"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"c_customer_id"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[webReturnsScanId] = getTableFilePaths(kWebReturns);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[customerAddrScanId] = getTableFilePaths(kCustomerAddress);
  result.dataFiles[customerScanId] = getTableFilePaths(kCustomer);
  result.dataFileFormat = format_;
  result.planName = "Q30";
  return result;
}

// Q63: 4-way join item, store_sales, date_dim, store with aggregation
// SELECT i_manager_id, sum(ss_sales_price) as sum_sales
// FROM item, store_sales, date_dim, store
// WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk
//   AND ss_store_sk = s_store_sk AND d_month_seq BETWEEN 1176 AND 1187
// GROUP BY i_manager_id, d_moy
// ORDER BY i_manager_id, sum_sales
// LIMIT 100
TpcdsPlan TpcdsQueryBuilder::getQ63Plan() const {
  std::vector<std::string> itemCols = {"i_item_sk", "i_manager_id"};
  std::vector<std::string> storeSalesCols = {
      "ss_item_sk", "ss_sold_date_sk", "ss_store_sk", "ss_sales_price"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_month_seq", "d_moy"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_store_name"};

  auto itemType = getRowType(kItem, itemCols);
  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeType = getRowType(kStore, storeCols);

  const auto& itemFileColumnNames = getFileColumnNames(kItem);
  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId itemScanId;
  core::PlanNodeId storeSalesScanId;
  core::PlanNodeId dateDimScanId;
  core::PlanNodeId storeScanId;

  // item
  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  // date_dim with filter d_month_seq between 1176 and 1187
  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_month_seq between 1176 and 1187"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  // store
  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  // Build join chain:
  // store_sales -> JOIN item -> JOIN date_dim -> JOIN store -> aggregate -> sort
  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin(
              {"ss_item_sk"},
              {"i_item_sk"},
              itemNode,
              "",
              {"ss_sold_date_sk", "ss_store_sk", "ss_sales_price", "i_manager_id"})
          .hashJoin(
              {"ss_sold_date_sk"},
              {"d_date_sk"},
              dateDimNode,
              "",
              {"ss_store_sk", "ss_sales_price", "i_manager_id", "d_moy"})
          .hashJoin(
              {"ss_store_sk"},
              {"s_store_sk"},
              storeNode,
              "",
              {"i_manager_id", "d_moy", "ss_sales_price"})
          .partialAggregation(
              {"i_manager_id", "d_moy"},
              {"sum(ss_sales_price) as sum_sales"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"i_manager_id", "sum_sales"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFileFormat = format_;
  result.planName = "Q63";
  return result;
}

// Q89: 4-way join item, store_sales, date_dim, store with aggregation (similar to Q63)
// SELECT i_category, i_class, i_brand, s_store_name, s_company_name,
//        d_moy, sum(ss_sales_price) as sum_sales
// FROM item, store_sales, date_dim, store
// WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk
//   AND ss_store_sk = s_store_sk AND d_year = 1999
// GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
// ORDER BY sum_sales, s_store_name
// LIMIT 100
TpcdsPlan TpcdsQueryBuilder::getQ89Plan() const {
  std::vector<std::string> itemCols = {
      "i_item_sk", "i_category", "i_class", "i_brand"};
  std::vector<std::string> storeSalesCols = {
      "ss_item_sk", "ss_sold_date_sk", "ss_store_sk", "ss_sales_price"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_moy"};
  std::vector<std::string> storeCols = {
      "s_store_sk", "s_store_name", "s_company_name"};

  auto itemType = getRowType(kItem, itemCols);
  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeType = getRowType(kStore, storeCols);

  const auto& itemFileColumnNames = getFileColumnNames(kItem);
  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId itemScanId;
  core::PlanNodeId storeSalesScanId;
  core::PlanNodeId dateDimScanId;
  core::PlanNodeId storeScanId;

  // item
  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  // date_dim with filter d_year = 1999
  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year = 1999"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  // store
  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  // Build join chain:
  // store_sales -> JOIN item -> JOIN date_dim -> JOIN store -> aggregate -> sort
  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin(
              {"ss_item_sk"},
              {"i_item_sk"},
              itemNode,
              "",
              {"ss_sold_date_sk", "ss_store_sk", "ss_sales_price",
               "i_category", "i_class", "i_brand"})
          .hashJoin(
              {"ss_sold_date_sk"},
              {"d_date_sk"},
              dateDimNode,
              "",
              {"ss_store_sk", "ss_sales_price", "i_category", "i_class",
               "i_brand", "d_moy"})
          .hashJoin(
              {"ss_store_sk"},
              {"s_store_sk"},
              storeNode,
              "",
              {"i_category", "i_class", "i_brand", "s_store_name",
               "s_company_name", "d_moy", "ss_sales_price"})
          .partialAggregation(
              {"i_category", "i_class", "i_brand", "s_store_name",
               "s_company_name", "d_moy"},
              {"sum(ss_sales_price) as sum_sales"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"sum_sales", "s_store_name"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFileFormat = format_;
  result.planName = "Q89";
  return result;
}

// Q7: Promotional sales analysis
// store_sales -> date_dim -> item -> customer_demographics -> promotion
// Low selectivity: promotion and demographics filters
TpcdsPlan TpcdsQueryBuilder::getQ7Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_item_sk", "ss_cdemo_sk", "ss_promo_sk",
      "ss_quantity", "ss_list_price", "ss_sales_price", "ss_coupon_amt"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year"};
  std::vector<std::string> itemCols = {"i_item_sk", "i_item_id"};
  std::vector<std::string> custDemoCols = {
      "cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"};
  std::vector<std::string> promoCols = {"p_promo_sk", "p_channel_email", "p_channel_event"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto itemType = getRowType(kItem, itemCols);
  auto custDemoType = getRowType(kCustomerDemographics, custDemoCols);
  auto promoType = getRowType(kPromotion, promoCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);
  const auto& cdFileColumnNames = getFileColumnNames(kCustomerDemographics);
  const auto& promoFileColumnNames = getFileColumnNames(kPromotion);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, dateDimScanId, itemScanId, custDemoScanId, promoScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year = 2000"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  auto custDemoNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomerDemographics, custDemoType, cdFileColumnNames,
                                     {"cd_gender = 'M'", "cd_marital_status = 'S'", "cd_education_status = 'College'"})
                          .capturePlanNodeId(custDemoScanId)
                          .planNode();

  auto promoNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kPromotion, promoType, promoFileColumnNames, {})
                       .capturePlanNodeId(promoScanId)
                       .filter("p_channel_email = 'N' OR p_channel_event = 'N'")
                       .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_item_sk", "ss_cdemo_sk", "ss_promo_sk", "ss_quantity",
                     "ss_list_price", "ss_sales_price", "ss_coupon_amt"})
          .hashJoin({"ss_item_sk"}, {"i_item_sk"}, itemNode, "",
                    {"ss_cdemo_sk", "ss_promo_sk", "ss_quantity", "ss_list_price",
                     "ss_sales_price", "ss_coupon_amt", "i_item_id"})
          .hashJoin({"ss_cdemo_sk"}, {"cd_demo_sk"}, custDemoNode, "",
                    {"ss_promo_sk", "ss_quantity", "ss_list_price", "ss_sales_price",
                     "ss_coupon_amt", "i_item_id"})
          .hashJoin({"ss_promo_sk"}, {"p_promo_sk"}, promoNode, "",
                    {"ss_quantity", "ss_list_price", "ss_sales_price", "ss_coupon_amt", "i_item_id"})
          .partialAggregation({"i_item_id"},
                              {"avg(ss_quantity) as agg1", "avg(ss_list_price) as agg2",
                               "avg(ss_coupon_amt) as agg3", "avg(ss_sales_price) as agg4"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"i_item_id"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFiles[custDemoScanId] = getTableFilePaths(kCustomerDemographics);
  result.dataFiles[promoScanId] = getTableFilePaths(kPromotion);
  result.dataFileFormat = format_;
  result.planName = "Q7";
  return result;
}

// Q13: store_sales with multiple dimension joins and filters
TpcdsPlan TpcdsQueryBuilder::getQ13Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_customer_sk", "ss_hdemo_sk", "ss_addr_sk",
      "ss_store_sk", "ss_quantity", "ss_ext_sales_price", "ss_ext_wholesale_cost",
      "ss_net_profit"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_store_name"};
  std::vector<std::string> hdCols = {"hd_demo_sk", "hd_dep_count"};
  std::vector<std::string> caCols = {"ca_address_sk", "ca_state"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeType = getRowType(kStore, storeCols);
  auto hdType = getRowType(kHouseholdDemographics, hdCols);
  auto caType = getRowType(kCustomerAddress, caCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);
  const auto& hdFileColumnNames = getFileColumnNames(kHouseholdDemographics);
  const auto& caFileColumnNames = getFileColumnNames(kCustomerAddress);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, dateDimScanId, storeScanId, hdScanId, caScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year = 2001"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  auto hdNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kHouseholdDemographics, hdType, hdFileColumnNames, {})
                    .capturePlanNodeId(hdScanId)
                    .planNode();

  auto caNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kCustomerAddress, caType, caFileColumnNames, {"ca_state IN ('TX', 'OH')"})
                    .capturePlanNodeId(caScanId)
                    .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_quantity",
                     "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_net_profit"})
          .hashJoin({"ss_store_sk"}, {"s_store_sk"}, storeNode, "",
                    {"ss_hdemo_sk", "ss_addr_sk", "ss_quantity", "ss_ext_sales_price",
                     "ss_ext_wholesale_cost", "ss_net_profit"})
          .hashJoin({"ss_hdemo_sk"}, {"hd_demo_sk"}, hdNode, "",
                    {"ss_addr_sk", "ss_quantity", "ss_ext_sales_price",
                     "ss_ext_wholesale_cost", "ss_net_profit"})
          .hashJoin({"ss_addr_sk"}, {"ca_address_sk"}, caNode, "",
                    {"ss_quantity", "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_net_profit"})
          .partialAggregation({},
                              {"avg(ss_quantity) as avg_qty", "avg(ss_ext_sales_price) as avg_sales",
                               "avg(ss_ext_wholesale_cost) as avg_cost", "sum(ss_net_profit) as sum_profit"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFiles[hdScanId] = getTableFilePaths(kHouseholdDemographics);
  result.dataFiles[caScanId] = getTableFilePaths(kCustomerAddress);
  result.dataFileFormat = format_;
  result.planName = "Q13";
  return result;
}

// Q15: catalog_sales with customer and customer_address
TpcdsPlan TpcdsQueryBuilder::getQ15Plan() const {
  std::vector<std::string> catalogSalesCols = {
      "cs_sold_date_sk", "cs_bill_customer_sk", "cs_sales_price"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_qoy"};
  std::vector<std::string> customerCols = {"c_customer_sk", "c_current_addr_sk"};
  std::vector<std::string> caCols = {"ca_address_sk", "ca_zip"};

  auto catalogSalesType = getRowType(kCatalogSales, catalogSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto customerType = getRowType(kCustomer, customerCols);
  auto caType = getRowType(kCustomerAddress, caCols);

  const auto& csFileColumnNames = getFileColumnNames(kCatalogSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& custFileColumnNames = getFileColumnNames(kCustomer);
  const auto& caFileColumnNames = getFileColumnNames(kCustomerAddress);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId catalogSalesScanId, dateDimScanId, customerScanId, caScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year = 2001", "d_qoy = 2"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto customerNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomer, customerType, custFileColumnNames, {})
                          .capturePlanNodeId(customerScanId)
                          .planNode();

  auto caNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kCustomerAddress, caType, caFileColumnNames, {})
                    .capturePlanNodeId(caScanId)
                    .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kCatalogSales, catalogSalesType, csFileColumnNames, {})
          .capturePlanNodeId(catalogSalesScanId)
          .hashJoin({"cs_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"cs_bill_customer_sk", "cs_sales_price"})
          .hashJoin({"cs_bill_customer_sk"}, {"c_customer_sk"}, customerNode, "",
                    {"cs_sales_price", "c_current_addr_sk"})
          .hashJoin({"c_current_addr_sk"}, {"ca_address_sk"}, caNode, "",
                    {"cs_sales_price", "ca_zip"})
          .partialAggregation({"ca_zip"}, {"sum(cs_sales_price) as sum_sales"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"ca_zip"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[catalogSalesScanId] = getTableFilePaths(kCatalogSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[customerScanId] = getTableFilePaths(kCustomer);
  result.dataFiles[caScanId] = getTableFilePaths(kCustomerAddress);
  result.dataFileFormat = format_;
  result.planName = "Q15";
  return result;
}

// Q19: store_sales with item, customer, customer_address - regional brand analysis
TpcdsPlan TpcdsQueryBuilder::getQ19Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_ext_sales_price"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_moy"};
  std::vector<std::string> itemCols = {"i_item_sk", "i_brand_id", "i_brand", "i_manufact_id", "i_manager_id"};
  std::vector<std::string> customerCols = {"c_customer_sk", "c_current_addr_sk"};
  std::vector<std::string> caCols = {"ca_address_sk", "ca_zip"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto itemType = getRowType(kItem, itemCols);
  auto customerType = getRowType(kCustomer, customerCols);
  auto caType = getRowType(kCustomerAddress, caCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);
  const auto& custFileColumnNames = getFileColumnNames(kCustomer);
  const auto& caFileColumnNames = getFileColumnNames(kCustomerAddress);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, dateDimScanId, itemScanId, customerScanId, caScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year = 1998", "d_moy = 11"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {"i_manager_id = 8"})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  auto customerNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomer, customerType, custFileColumnNames, {})
                          .capturePlanNodeId(customerScanId)
                          .planNode();

  auto caNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kCustomerAddress, caType, caFileColumnNames, {})
                    .capturePlanNodeId(caScanId)
                    .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_item_sk", "ss_customer_sk", "ss_ext_sales_price"})
          .hashJoin({"ss_item_sk"}, {"i_item_sk"}, itemNode, "",
                    {"ss_customer_sk", "ss_ext_sales_price", "i_brand_id", "i_brand", "i_manufact_id"})
          .hashJoin({"ss_customer_sk"}, {"c_customer_sk"}, customerNode, "",
                    {"ss_ext_sales_price", "i_brand_id", "i_brand", "i_manufact_id", "c_current_addr_sk"})
          .hashJoin({"c_current_addr_sk"}, {"ca_address_sk"}, caNode, "",
                    {"ss_ext_sales_price", "i_brand_id", "i_brand", "i_manufact_id", "ca_zip"})
          .partialAggregation({"i_brand_id", "i_brand", "i_manufact_id"},
                              {"sum(ss_ext_sales_price) as ext_price"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"ext_price DESC", "i_brand", "i_brand_id", "i_manufact_id"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFiles[customerScanId] = getTableFilePaths(kCustomer);
  result.dataFiles[caScanId] = getTableFilePaths(kCustomerAddress);
  result.dataFileFormat = format_;
  result.planName = "Q19";
  return result;
}

// Q25: store_sales with store_returns (low selectivity - only returned items)
// This is promising for hybrid as returns are ~2-5% of sales
TpcdsPlan TpcdsQueryBuilder::getQ25Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_item_sk", "ss_ticket_number", "ss_customer_sk",
      "ss_store_sk", "ss_net_profit"};
  std::vector<std::string> storeReturnsCols = {
      "sr_returned_date_sk", "sr_item_sk", "sr_ticket_number", "sr_customer_sk",
      "sr_store_sk", "sr_net_loss"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_store_id", "s_store_name"};
  std::vector<std::string> itemCols = {"i_item_sk", "i_item_id", "i_item_desc"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto storeReturnsType = getRowType(kStoreReturns, storeReturnsCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeType = getRowType(kStore, storeCols);
  auto itemType = getRowType(kItem, itemCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& srFileColumnNames = getFileColumnNames(kStoreReturns);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, storeReturnsScanId, dateDim1ScanId, dateDim2ScanId;
  core::PlanNodeId storeScanId, itemScanId;

  auto dateDim1Node = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year = 2000"})
                          .capturePlanNodeId(dateDim1ScanId)
                          .planNode();

  auto dateDim2Node = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year = 2001"})
                          .capturePlanNodeId(dateDim2ScanId)
                          .planNode();

  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  // Build store_returns subquery first
  auto storeReturnsNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                              .tableScan(kStoreReturns, storeReturnsType, srFileColumnNames, {})
                              .capturePlanNodeId(storeReturnsScanId)
                              .hashJoin({"sr_returned_date_sk"}, {"d_date_sk"}, dateDim2Node, "",
                                        {"sr_item_sk", "sr_ticket_number", "sr_customer_sk",
                                         "sr_store_sk", "sr_net_loss"})
                              .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDim1Node, "",
                    {"ss_item_sk", "ss_ticket_number", "ss_customer_sk", "ss_store_sk", "ss_net_profit"})
          // Join with returns (this is the low-selectivity join!)
          .hashJoin({"ss_item_sk", "ss_ticket_number", "ss_customer_sk"},
                    {"sr_item_sk", "sr_ticket_number", "sr_customer_sk"},
                    storeReturnsNode, "",
                    {"ss_store_sk", "ss_net_profit", "sr_net_loss"})
          .hashJoin({"ss_store_sk"}, {"s_store_sk"}, storeNode, "",
                    {"ss_net_profit", "sr_net_loss", "s_store_id", "s_store_name"})
          .partialAggregation({"s_store_id", "s_store_name"},
                              {"sum(ss_net_profit) as store_profit", "sum(sr_net_loss) as store_loss"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"s_store_id"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[storeReturnsScanId] = getTableFilePaths(kStoreReturns);
  result.dataFiles[dateDim1ScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[dateDim2ScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFileFormat = format_;
  result.planName = "Q25";
  return result;
}

// Q26: catalog_sales promotional analysis (similar to Q7 but for catalog)
TpcdsPlan TpcdsQueryBuilder::getQ26Plan() const {
  std::vector<std::string> catalogSalesCols = {
      "cs_sold_date_sk", "cs_item_sk", "cs_bill_cdemo_sk", "cs_promo_sk",
      "cs_quantity", "cs_list_price", "cs_sales_price", "cs_coupon_amt"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year"};
  std::vector<std::string> itemCols = {"i_item_sk", "i_item_id"};
  std::vector<std::string> custDemoCols = {
      "cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"};
  std::vector<std::string> promoCols = {"p_promo_sk", "p_channel_email"};

  auto catalogSalesType = getRowType(kCatalogSales, catalogSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto itemType = getRowType(kItem, itemCols);
  auto custDemoType = getRowType(kCustomerDemographics, custDemoCols);
  auto promoType = getRowType(kPromotion, promoCols);

  const auto& csFileColumnNames = getFileColumnNames(kCatalogSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);
  const auto& cdFileColumnNames = getFileColumnNames(kCustomerDemographics);
  const auto& promoFileColumnNames = getFileColumnNames(kPromotion);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId catalogSalesScanId, dateDimScanId, itemScanId, custDemoScanId, promoScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year = 2000"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  auto custDemoNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomerDemographics, custDemoType, cdFileColumnNames,
                                     {"cd_gender = 'M'", "cd_marital_status = 'S'", "cd_education_status = 'College'"})
                          .capturePlanNodeId(custDemoScanId)
                          .planNode();

  auto promoNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kPromotion, promoType, promoFileColumnNames, {"p_channel_email = 'N'"})
                       .capturePlanNodeId(promoScanId)
                       .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kCatalogSales, catalogSalesType, csFileColumnNames, {})
          .capturePlanNodeId(catalogSalesScanId)
          .hashJoin({"cs_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"cs_item_sk", "cs_bill_cdemo_sk", "cs_promo_sk", "cs_quantity",
                     "cs_list_price", "cs_sales_price", "cs_coupon_amt"})
          .hashJoin({"cs_item_sk"}, {"i_item_sk"}, itemNode, "",
                    {"cs_bill_cdemo_sk", "cs_promo_sk", "cs_quantity", "cs_list_price",
                     "cs_sales_price", "cs_coupon_amt", "i_item_id"})
          .hashJoin({"cs_bill_cdemo_sk"}, {"cd_demo_sk"}, custDemoNode, "",
                    {"cs_promo_sk", "cs_quantity", "cs_list_price", "cs_sales_price",
                     "cs_coupon_amt", "i_item_id"})
          .hashJoin({"cs_promo_sk"}, {"p_promo_sk"}, promoNode, "",
                    {"cs_quantity", "cs_list_price", "cs_sales_price", "cs_coupon_amt", "i_item_id"})
          .partialAggregation({"i_item_id"},
                              {"avg(cs_quantity) as agg1", "avg(cs_list_price) as agg2",
                               "avg(cs_coupon_amt) as agg3", "avg(cs_sales_price) as agg4"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"i_item_id"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[catalogSalesScanId] = getTableFilePaths(kCatalogSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFiles[custDemoScanId] = getTableFilePaths(kCustomerDemographics);
  result.dataFiles[promoScanId] = getTableFilePaths(kPromotion);
  result.dataFileFormat = format_;
  result.planName = "Q26";
  return result;
}

// Q29: catalog_sales with catalog_returns - another low selectivity opportunity
TpcdsPlan TpcdsQueryBuilder::getQ29Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_item_sk", "ss_ticket_number", "ss_customer_sk",
      "ss_quantity"};
  std::vector<std::string> storeReturnsCols = {
      "sr_returned_date_sk", "sr_item_sk", "sr_ticket_number", "sr_customer_sk",
      "sr_return_quantity"};
  std::vector<std::string> catalogSalesCols = {
      "cs_sold_date_sk", "cs_item_sk", "cs_bill_customer_sk", "cs_quantity"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_moy"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_store_id", "s_store_name"};
  std::vector<std::string> itemCols = {"i_item_sk", "i_item_id", "i_item_desc"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto storeReturnsType = getRowType(kStoreReturns, storeReturnsCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto itemType = getRowType(kItem, itemCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& srFileColumnNames = getFileColumnNames(kStoreReturns);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, storeReturnsScanId, dateDim1ScanId, dateDim2ScanId;
  core::PlanNodeId itemScanId;

  auto dateDim1Node = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year = 1999", "d_moy = 9"})
                          .capturePlanNodeId(dateDim1ScanId)
                          .planNode();

  auto dateDim2Node = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year IN (1999, 2000, 2001)"})
                          .capturePlanNodeId(dateDim2ScanId)
                          .planNode();

  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  // store_returns with date filter
  auto storeReturnsNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                              .tableScan(kStoreReturns, storeReturnsType, srFileColumnNames, {})
                              .capturePlanNodeId(storeReturnsScanId)
                              .hashJoin({"sr_returned_date_sk"}, {"d_date_sk"}, dateDim2Node, "",
                                        {"sr_item_sk", "sr_ticket_number", "sr_customer_sk", "sr_return_quantity"})
                              .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDim1Node, "",
                    {"ss_item_sk", "ss_ticket_number", "ss_customer_sk", "ss_quantity"})
          .hashJoin({"ss_item_sk", "ss_ticket_number", "ss_customer_sk"},
                    {"sr_item_sk", "sr_ticket_number", "sr_customer_sk"},
                    storeReturnsNode, "",
                    {"ss_item_sk", "ss_quantity", "sr_return_quantity"})
          .hashJoin({"ss_item_sk"}, {"i_item_sk"}, itemNode, "",
                    {"ss_quantity", "sr_return_quantity", "i_item_id", "i_item_desc"})
          .partialAggregation({"i_item_id", "i_item_desc"},
                              {"sum(ss_quantity) as store_qty", "sum(sr_return_quantity) as return_qty"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"i_item_id", "i_item_desc"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[storeReturnsScanId] = getTableFilePaths(kStoreReturns);
  result.dataFiles[dateDim1ScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[dateDim2ScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFileFormat = format_;
  result.planName = "Q29";
  return result;
}

// Q42: Simple date_dim, store_sales, item (like Q3 but simpler)
TpcdsPlan TpcdsQueryBuilder::getQ42Plan() const {
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_moy"};
  std::vector<std::string> storeSalesCols = {"ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"};
  std::vector<std::string> itemCols = {"i_item_sk", "i_category_id", "i_category", "i_manager_id"};

  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto itemType = getRowType(kItem, itemCols);

  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId dateDimScanId, storeSalesScanId, itemScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year = 2000", "d_moy = 11"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {"i_manager_id = 1"})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_item_sk", "ss_ext_sales_price", "d_year"})
          .hashJoin({"ss_item_sk"}, {"i_item_sk"}, itemNode, "",
                    {"d_year", "i_category_id", "i_category", "ss_ext_sales_price"})
          .partialAggregation({"d_year", "i_category_id", "i_category"},
                              {"sum(ss_ext_sales_price) as sum_sales"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"sum_sales DESC", "d_year", "i_category_id", "i_category"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFileFormat = format_;
  result.planName = "Q42";
  return result;
}

// Q43: date_dim, store_sales, store - day of week sales by store
TpcdsPlan TpcdsQueryBuilder::getQ43Plan() const {
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_day_name"};
  std::vector<std::string> storeSalesCols = {"ss_sold_date_sk", "ss_store_sk", "ss_sales_price"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_store_id", "s_store_name", "s_gmt_offset"};

  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto storeType = getRowType(kStore, storeCols);

  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId dateDimScanId, storeSalesScanId, storeScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames, {"d_year = 2000"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {"s_gmt_offset = -5"})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_store_sk", "ss_sales_price", "d_day_name"})
          .hashJoin({"ss_store_sk"}, {"s_store_sk"}, storeNode, "",
                    {"ss_sales_price", "d_day_name", "s_store_id", "s_store_name"})
          .partialAggregation({"s_store_name", "s_store_id", "d_day_name"},
                              {"sum(ss_sales_price) as day_sales"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"s_store_name", "s_store_id", "d_day_name"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFileFormat = format_;
  result.planName = "Q43";
  return result;
}

// Q46: store_sales with multiple dimension joins and customer info
TpcdsPlan TpcdsQueryBuilder::getQ46Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_customer_sk", "ss_hdemo_sk", "ss_addr_sk",
      "ss_store_sk", "ss_ticket_number", "ss_coupon_amt", "ss_net_profit"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_dow"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_city"};
  std::vector<std::string> hdCols = {"hd_demo_sk", "hd_dep_count", "hd_vehicle_count"};
  std::vector<std::string> customerCols = {"c_customer_sk", "c_current_addr_sk", "c_first_name", "c_last_name"};
  std::vector<std::string> caCols = {"ca_address_sk", "ca_city"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeType = getRowType(kStore, storeCols);
  auto hdType = getRowType(kHouseholdDemographics, hdCols);
  auto customerType = getRowType(kCustomer, customerCols);
  auto caType = getRowType(kCustomerAddress, caCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);
  const auto& hdFileColumnNames = getFileColumnNames(kHouseholdDemographics);
  const auto& custFileColumnNames = getFileColumnNames(kCustomer);
  const auto& caFileColumnNames = getFileColumnNames(kCustomerAddress);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, dateDimScanId, storeScanId, hdScanId, customerScanId, caScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year IN (1999, 2000, 2001)", "d_dow IN (6, 0)"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {"s_city IN ('Midway', 'Fairview')"})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  auto hdNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kHouseholdDemographics, hdType, hdFileColumnNames, {})
                    .capturePlanNodeId(hdScanId)
                    .planNode();

  auto customerNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomer, customerType, custFileColumnNames, {})
                          .capturePlanNodeId(customerScanId)
                          .planNode();

  auto caNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kCustomerAddress, caType, caFileColumnNames, {})
                    .capturePlanNodeId(caScanId)
                    .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_customer_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk",
                     "ss_ticket_number", "ss_coupon_amt", "ss_net_profit"})
          .hashJoin({"ss_store_sk"}, {"s_store_sk"}, storeNode, "",
                    {"ss_customer_sk", "ss_hdemo_sk", "ss_ticket_number",
                     "ss_coupon_amt", "ss_net_profit", "s_city"})
          .hashJoin({"ss_hdemo_sk"}, {"hd_demo_sk"}, hdNode, "",
                    {"ss_customer_sk", "ss_ticket_number", "ss_coupon_amt", "ss_net_profit", "s_city"})
          .hashJoin({"ss_customer_sk"}, {"c_customer_sk"}, customerNode, "",
                    {"ss_ticket_number", "ss_coupon_amt", "ss_net_profit", "s_city",
                     "c_current_addr_sk", "c_first_name", "c_last_name"})
          .hashJoin({"c_current_addr_sk"}, {"ca_address_sk"}, caNode, "",
                    {"ss_ticket_number", "ss_coupon_amt", "ss_net_profit", "s_city",
                     "c_first_name", "c_last_name", "ca_city"})
          .partialAggregation({"c_last_name", "c_first_name", "ca_city", "s_city", "ss_ticket_number"},
                              {"sum(ss_coupon_amt) as amt", "sum(ss_net_profit) as profit"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"c_last_name", "c_first_name", "ca_city", "s_city", "ss_ticket_number"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFiles[hdScanId] = getTableFilePaths(kHouseholdDemographics);
  result.dataFiles[customerScanId] = getTableFilePaths(kCustomer);
  result.dataFiles[caScanId] = getTableFilePaths(kCustomerAddress);
  result.dataFileFormat = format_;
  result.planName = "Q46";
  return result;
}

// Q52: Simple quarterly sales by department (like Q42)
TpcdsPlan TpcdsQueryBuilder::getQ52Plan() const {
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_moy"};
  std::vector<std::string> storeSalesCols = {"ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"};
  std::vector<std::string> itemCols = {"i_item_sk", "i_brand_id", "i_brand"};

  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto itemType = getRowType(kItem, itemCols);

  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId dateDimScanId, storeSalesScanId, itemScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year = 2000", "d_moy = 11"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_item_sk", "ss_ext_sales_price", "d_year"})
          .hashJoin({"ss_item_sk"}, {"i_item_sk"}, itemNode, "",
                    {"d_year", "i_brand_id", "i_brand", "ss_ext_sales_price"})
          .partialAggregation({"d_year", "i_brand_id", "i_brand"},
                              {"sum(ss_ext_sales_price) as ext_price"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"d_year", "ext_price DESC", "i_brand_id"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFileFormat = format_;
  result.planName = "Q52";
  return result;
}

// Q53: Sales by item with store and date filters
TpcdsPlan TpcdsQueryBuilder::getQ53Plan() const {
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_month_seq"};
  std::vector<std::string> storeSalesCols = {"ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"};
  std::vector<std::string> itemCols = {"i_item_sk", "i_manufact_id"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_store_name", "s_gmt_offset"};

  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto itemType = getRowType(kItem, itemCols);
  auto storeType = getRowType(kStore, storeCols);

  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId dateDimScanId, storeSalesScanId, itemScanId, storeScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_month_seq IN (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {"i_manufact_id IN (128, 129, 130, 131)"})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_item_sk", "ss_store_sk", "ss_sales_price", "d_month_seq"})
          .hashJoin({"ss_item_sk"}, {"i_item_sk"}, itemNode, "",
                    {"ss_store_sk", "ss_sales_price", "d_month_seq", "i_manufact_id"})
          .hashJoin({"ss_store_sk"}, {"s_store_sk"}, storeNode, "",
                    {"ss_sales_price", "d_month_seq", "i_manufact_id", "s_store_name"})
          .partialAggregation({"i_manufact_id", "d_month_seq"},
                              {"sum(ss_sales_price) as sum_sales"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"sum_sales", "i_manufact_id", "d_month_seq"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFileFormat = format_;
  result.planName = "Q53";
  return result;
}

// Q55: Simple brand sales in a time period
TpcdsPlan TpcdsQueryBuilder::getQ55Plan() const {
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_moy"};
  std::vector<std::string> storeSalesCols = {"ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"};
  std::vector<std::string> itemCols = {"i_item_sk", "i_brand_id", "i_brand", "i_manager_id"};

  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto itemType = getRowType(kItem, itemCols);

  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& itemFileColumnNames = getFileColumnNames(kItem);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId dateDimScanId, storeSalesScanId, itemScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year = 1999", "d_moy = 11"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto itemNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(kItem, itemType, itemFileColumnNames, {"i_manager_id = 28"})
                      .capturePlanNodeId(itemScanId)
                      .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_item_sk", "ss_ext_sales_price"})
          .hashJoin({"ss_item_sk"}, {"i_item_sk"}, itemNode, "",
                    {"ss_ext_sales_price", "i_brand_id", "i_brand"})
          .partialAggregation({"i_brand_id", "i_brand"},
                              {"sum(ss_ext_sales_price) as ext_price"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"ext_price DESC", "i_brand_id"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[itemScanId] = getTableFilePaths(kItem);
  result.dataFileFormat = format_;
  result.planName = "Q55";
  return result;
}

// Q68: Customer purchases detail with multiple dimensions
TpcdsPlan TpcdsQueryBuilder::getQ68Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_customer_sk", "ss_hdemo_sk", "ss_addr_sk",
      "ss_store_sk", "ss_ticket_number", "ss_ext_sales_price", "ss_ext_list_price", "ss_ext_tax"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_dom"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_city"};
  std::vector<std::string> hdCols = {"hd_demo_sk", "hd_dep_count", "hd_vehicle_count"};
  std::vector<std::string> customerCols = {"c_customer_sk", "c_current_addr_sk", "c_first_name", "c_last_name"};
  std::vector<std::string> caCols = {"ca_address_sk", "ca_city"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeType = getRowType(kStore, storeCols);
  auto hdType = getRowType(kHouseholdDemographics, hdCols);
  auto customerType = getRowType(kCustomer, customerCols);
  auto caType = getRowType(kCustomerAddress, caCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);
  const auto& hdFileColumnNames = getFileColumnNames(kHouseholdDemographics);
  const auto& custFileColumnNames = getFileColumnNames(kCustomer);
  const auto& caFileColumnNames = getFileColumnNames(kCustomerAddress);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, dateDimScanId, storeScanId, hdScanId, customerScanId, caScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year IN (1999, 2000, 2001)", "d_dom BETWEEN 1 AND 2"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {"s_city IN ('Midway', 'Fairview')"})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  auto hdNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kHouseholdDemographics, hdType, hdFileColumnNames, {})
                    .capturePlanNodeId(hdScanId)
                    .filter("hd_dep_count = 4 OR hd_vehicle_count = 3")
                    .planNode();

  auto customerNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomer, customerType, custFileColumnNames, {})
                          .capturePlanNodeId(customerScanId)
                          .planNode();

  auto caNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kCustomerAddress, caType, caFileColumnNames, {})
                    .capturePlanNodeId(caScanId)
                    .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_customer_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk",
                     "ss_ticket_number", "ss_ext_sales_price", "ss_ext_list_price", "ss_ext_tax"})
          .hashJoin({"ss_store_sk"}, {"s_store_sk"}, storeNode, "",
                    {"ss_customer_sk", "ss_hdemo_sk", "ss_ticket_number",
                     "ss_ext_sales_price", "ss_ext_list_price", "ss_ext_tax", "s_city"})
          .hashJoin({"ss_hdemo_sk"}, {"hd_demo_sk"}, hdNode, "",
                    {"ss_customer_sk", "ss_ticket_number", "ss_ext_sales_price",
                     "ss_ext_list_price", "ss_ext_tax", "s_city"})
          .hashJoin({"ss_customer_sk"}, {"c_customer_sk"}, customerNode, "",
                    {"ss_ticket_number", "ss_ext_sales_price", "ss_ext_list_price",
                     "ss_ext_tax", "s_city", "c_current_addr_sk", "c_first_name", "c_last_name"})
          .hashJoin({"c_current_addr_sk"}, {"ca_address_sk"}, caNode, "",
                    {"ss_ticket_number", "ss_ext_sales_price", "ss_ext_list_price",
                     "ss_ext_tax", "s_city", "c_first_name", "c_last_name", "ca_city"})
          .partialAggregation({"c_last_name", "c_first_name", "ca_city", "s_city", "ss_ticket_number"},
                              {"sum(ss_ext_sales_price) as bought_city_amt",
                               "sum(ss_ext_list_price) as list_amt", "sum(ss_ext_tax) as tax_amt"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"c_last_name", "c_first_name", "ca_city", "s_city", "ss_ticket_number"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFiles[hdScanId] = getTableFilePaths(kHouseholdDemographics);
  result.dataFiles[customerScanId] = getTableFilePaths(kCustomer);
  result.dataFiles[caScanId] = getTableFilePaths(kCustomerAddress);
  result.dataFileFormat = format_;
  result.planName = "Q68";
  return result;
}

// Q73: Store ticket analysis
TpcdsPlan TpcdsQueryBuilder::getQ73Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_customer_sk", "ss_hdemo_sk", "ss_store_sk", "ss_ticket_number"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_dom"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_county"};
  std::vector<std::string> hdCols = {"hd_demo_sk", "hd_buy_potential", "hd_dep_count", "hd_vehicle_count"};
  std::vector<std::string> customerCols = {"c_customer_sk", "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeType = getRowType(kStore, storeCols);
  auto hdType = getRowType(kHouseholdDemographics, hdCols);
  auto customerType = getRowType(kCustomer, customerCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);
  const auto& hdFileColumnNames = getFileColumnNames(kHouseholdDemographics);
  const auto& custFileColumnNames = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, dateDimScanId, storeScanId, hdScanId, customerScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year IN (1999, 2000, 2001)", "d_dom BETWEEN 1 AND 2"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {"s_county IN ('Williamson County')"})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  auto hdNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kHouseholdDemographics, hdType, hdFileColumnNames,
                               {"hd_buy_potential = '>10000'", "hd_vehicle_count > 0"})
                    .capturePlanNodeId(hdScanId)
                    .planNode();

  auto customerNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomer, customerType, custFileColumnNames, {})
                          .capturePlanNodeId(customerScanId)
                          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_customer_sk", "ss_hdemo_sk", "ss_store_sk", "ss_ticket_number"})
          .hashJoin({"ss_store_sk"}, {"s_store_sk"}, storeNode, "",
                    {"ss_customer_sk", "ss_hdemo_sk", "ss_ticket_number", "s_county"})
          .hashJoin({"ss_hdemo_sk"}, {"hd_demo_sk"}, hdNode, "",
                    {"ss_customer_sk", "ss_ticket_number", "s_county"})
          .partialAggregation({"ss_customer_sk", "ss_ticket_number", "s_county"},
                              {"count(1) as cnt"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .hashJoin({"ss_customer_sk"}, {"c_customer_sk"}, customerNode, "",
                    {"ss_ticket_number", "s_county", "cnt",
                     "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag"})
          .orderBy({"cnt DESC", "c_last_name"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFiles[hdScanId] = getTableFilePaths(kHouseholdDemographics);
  result.dataFiles[customerScanId] = getTableFilePaths(kCustomer);
  result.dataFileFormat = format_;
  result.planName = "Q73";
  return result;
}

// Q79: store_sales with customer info - profitability by customer
TpcdsPlan TpcdsQueryBuilder::getQ79Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_date_sk", "ss_customer_sk", "ss_hdemo_sk",
      "ss_store_sk", "ss_ticket_number", "ss_net_profit"};
  std::vector<std::string> dateDimCols = {"d_date_sk", "d_year", "d_dow"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_number_employees", "s_city"};
  std::vector<std::string> hdCols = {"hd_demo_sk", "hd_dep_count", "hd_vehicle_count"};
  std::vector<std::string> customerCols = {"c_customer_sk", "c_first_name", "c_last_name"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto dateDimType = getRowType(kDateDim, dateDimCols);
  auto storeType = getRowType(kStore, storeCols);
  auto hdType = getRowType(kHouseholdDemographics, hdCols);
  auto customerType = getRowType(kCustomer, customerCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& ddFileColumnNames = getFileColumnNames(kDateDim);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);
  const auto& hdFileColumnNames = getFileColumnNames(kHouseholdDemographics);
  const auto& custFileColumnNames = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, dateDimScanId, storeScanId, hdScanId, customerScanId;

  auto dateDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kDateDim, dateDimType, ddFileColumnNames,
                                    {"d_year = 2000", "d_dow IN (6, 0)"})
                         .capturePlanNodeId(dateDimScanId)
                         .planNode();

  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {"s_number_employees BETWEEN 200 AND 295"})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  auto hdNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kHouseholdDemographics, hdType, hdFileColumnNames, {})
                    .capturePlanNodeId(hdScanId)
                    .filter("hd_dep_count = 6 OR hd_vehicle_count > 2")
                    .planNode();

  auto customerNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                          .tableScan(kCustomer, customerType, custFileColumnNames, {})
                          .capturePlanNodeId(customerScanId)
                          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_date_sk"}, {"d_date_sk"}, dateDimNode, "",
                    {"ss_customer_sk", "ss_hdemo_sk", "ss_store_sk", "ss_ticket_number", "ss_net_profit"})
          .hashJoin({"ss_store_sk"}, {"s_store_sk"}, storeNode, "",
                    {"ss_customer_sk", "ss_hdemo_sk", "ss_ticket_number", "ss_net_profit", "s_city"})
          .hashJoin({"ss_hdemo_sk"}, {"hd_demo_sk"}, hdNode, "",
                    {"ss_customer_sk", "ss_ticket_number", "ss_net_profit", "s_city"})
          .hashJoin({"ss_customer_sk"}, {"c_customer_sk"}, customerNode, "",
                    {"ss_ticket_number", "ss_net_profit", "s_city", "c_first_name", "c_last_name"})
          .partialAggregation({"c_last_name", "c_first_name", "s_city", "ss_ticket_number"},
                              {"sum(ss_net_profit) as profit"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"c_last_name", "c_first_name", "s_city", "profit"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[dateDimScanId] = getTableFilePaths(kDateDim);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFiles[hdScanId] = getTableFilePaths(kHouseholdDemographics);
  result.dataFiles[customerScanId] = getTableFilePaths(kCustomer);
  result.dataFileFormat = format_;
  result.planName = "Q79";
  return result;
}

// Q96: Simple store_sales transaction count
TpcdsPlan TpcdsQueryBuilder::getQ96Plan() const {
  std::vector<std::string> storeSalesCols = {
      "ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"};
  std::vector<std::string> timeDimCols = {"t_time_sk", "t_hour", "t_minute"};
  std::vector<std::string> hdCols = {"hd_demo_sk", "hd_dep_count"};
  std::vector<std::string> storeCols = {"s_store_sk", "s_store_name"};

  auto storeSalesType = getRowType(kStoreSales, storeSalesCols);
  auto timeDimType = getRowType(kTimeDim, timeDimCols);
  auto hdType = getRowType(kHouseholdDemographics, hdCols);
  auto storeType = getRowType(kStore, storeCols);

  const auto& ssFileColumnNames = getFileColumnNames(kStoreSales);
  const auto& tdFileColumnNames = getFileColumnNames(kTimeDim);
  const auto& hdFileColumnNames = getFileColumnNames(kHouseholdDemographics);
  const auto& storeFileColumnNames = getFileColumnNames(kStore);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId storeSalesScanId, timeDimScanId, hdScanId, storeScanId;

  auto timeDimNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                         .tableScan(kTimeDim, timeDimType, tdFileColumnNames,
                                    {"t_hour = 20", "t_minute >= 30"})
                         .capturePlanNodeId(timeDimScanId)
                         .planNode();

  auto hdNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(kHouseholdDemographics, hdType, hdFileColumnNames, {"hd_dep_count = 7"})
                    .capturePlanNodeId(hdScanId)
                    .planNode();

  auto storeNode = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(kStore, storeType, storeFileColumnNames, {"s_store_name = 'ese'"})
                       .capturePlanNodeId(storeScanId)
                       .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kStoreSales, storeSalesType, ssFileColumnNames, {})
          .capturePlanNodeId(storeSalesScanId)
          .hashJoin({"ss_sold_time_sk"}, {"t_time_sk"}, timeDimNode, "",
                    {"ss_hdemo_sk", "ss_store_sk"})
          .hashJoin({"ss_hdemo_sk"}, {"hd_demo_sk"}, hdNode, "",
                    {"ss_store_sk"})
          .hashJoin({"ss_store_sk"}, {"s_store_sk"}, storeNode, "",
                    {})
          .partialAggregation({}, {"count(1) as cnt"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"cnt"}, false)
          .planNode();

  TpcdsPlan result;
  result.plan = std::move(plan);
  result.dataFiles[storeSalesScanId] = getTableFilePaths(kStoreSales);
  result.dataFiles[timeDimScanId] = getTableFilePaths(kTimeDim);
  result.dataFiles[hdScanId] = getTableFilePaths(kHouseholdDemographics);
  result.dataFiles[storeScanId] = getTableFilePaths(kStore);
  result.dataFileFormat = format_;
  result.planName = "Q96";
  return result;
}

const std::vector<std::string> TpcdsQueryBuilder::kTableNames_ = {
    kStoreSales,
    kStoreReturns,
    kCatalogSales,
    kCatalogReturns,
    kWebSales,
    kWebReturns,
    kInventory,
    kCustomer,
    kCustomerAddress,
    kCustomerDemographics,
    kDateDim,
    kTimeDim,
    kItem,
    kStore,
    kPromotion,
    kHouseholdDemographics,
    kWarehouse,
    kShipMode,
    kReason,
    kIncomeBand,
    kCallCenter,
    kCatalogPage,
    kWebPage,
    kWebSite};

const std::unordered_map<std::string, std::vector<std::string>>
    TpcdsQueryBuilder::kTables_ = {
        // Fact tables
        {"store_sales",
         {"ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk",
          "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk",
          "ss_promo_sk", "ss_ticket_number", "ss_quantity", "ss_wholesale_cost",
          "ss_list_price", "ss_sales_price", "ss_ext_discount_amt",
          "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_ext_list_price",
          "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax",
          "ss_net_profit"}},
        {"store_returns",
         {"sr_returned_date_sk", "sr_return_time_sk", "sr_item_sk",
          "sr_customer_sk", "sr_cdemo_sk", "sr_hdemo_sk", "sr_addr_sk",
          "sr_store_sk", "sr_reason_sk", "sr_ticket_number", "sr_return_quantity",
          "sr_return_amt", "sr_return_tax", "sr_return_amt_inc_tax", "sr_fee",
          "sr_return_ship_cost", "sr_refunded_cash", "sr_reversed_charge",
          "sr_store_credit", "sr_net_loss"}},
        {"catalog_sales",
         {"cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_date_sk",
          "cs_bill_customer_sk", "cs_bill_cdemo_sk", "cs_bill_hdemo_sk",
          "cs_bill_addr_sk", "cs_ship_customer_sk", "cs_ship_cdemo_sk",
          "cs_ship_hdemo_sk", "cs_ship_addr_sk", "cs_call_center_sk",
          "cs_catalog_page_sk", "cs_ship_mode_sk", "cs_warehouse_sk",
          "cs_item_sk", "cs_promo_sk", "cs_order_number", "cs_quantity",
          "cs_wholesale_cost", "cs_list_price", "cs_sales_price",
          "cs_ext_discount_amt", "cs_ext_sales_price", "cs_ext_wholesale_cost",
          "cs_ext_list_price", "cs_ext_tax", "cs_coupon_amt", "cs_ext_ship_cost",
          "cs_net_paid", "cs_net_paid_inc_tax", "cs_net_paid_inc_ship",
          "cs_net_paid_inc_ship_tax", "cs_net_profit"}},
        {"catalog_returns",
         {"cr_returned_date_sk", "cr_returned_time_sk", "cr_item_sk",
          "cr_refunded_customer_sk", "cr_refunded_cdemo_sk",
          "cr_refunded_hdemo_sk", "cr_refunded_addr_sk",
          "cr_returning_customer_sk", "cr_returning_cdemo_sk",
          "cr_returning_hdemo_sk", "cr_returning_addr_sk", "cr_call_center_sk",
          "cr_catalog_page_sk", "cr_ship_mode_sk", "cr_warehouse_sk",
          "cr_reason_sk", "cr_order_number", "cr_return_quantity",
          "cr_return_amount", "cr_return_tax", "cr_return_amt_inc_tax", "cr_fee",
          "cr_return_ship_cost", "cr_refunded_cash", "cr_reversed_charge",
          "cr_store_credit", "cr_net_loss"}},
        {"web_sales",
         {"ws_sold_date_sk", "ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk",
          "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk",
          "ws_bill_addr_sk", "ws_ship_customer_sk", "ws_ship_cdemo_sk",
          "ws_ship_hdemo_sk", "ws_ship_addr_sk", "ws_web_page_sk",
          "ws_web_site_sk", "ws_ship_mode_sk", "ws_warehouse_sk", "ws_promo_sk",
          "ws_order_number", "ws_quantity", "ws_wholesale_cost", "ws_list_price",
          "ws_sales_price", "ws_ext_discount_amt", "ws_ext_sales_price",
          "ws_ext_wholesale_cost", "ws_ext_list_price", "ws_ext_tax",
          "ws_coupon_amt", "ws_ext_ship_cost", "ws_net_paid",
          "ws_net_paid_inc_tax", "ws_net_paid_inc_ship",
          "ws_net_paid_inc_ship_tax", "ws_net_profit"}},
        {"web_returns",
         {"wr_returned_date_sk", "wr_returned_time_sk", "wr_item_sk",
          "wr_refunded_customer_sk", "wr_refunded_cdemo_sk",
          "wr_refunded_hdemo_sk", "wr_refunded_addr_sk",
          "wr_returning_customer_sk", "wr_returning_cdemo_sk",
          "wr_returning_hdemo_sk", "wr_returning_addr_sk", "wr_web_page_sk",
          "wr_reason_sk", "wr_order_number", "wr_return_quantity",
          "wr_return_amt", "wr_return_tax", "wr_return_amt_inc_tax", "wr_fee",
          "wr_return_ship_cost", "wr_refunded_cash", "wr_reversed_charge",
          "wr_account_credit", "wr_net_loss"}},
        {"inventory",
         {"inv_date_sk", "inv_item_sk", "inv_warehouse_sk",
          "inv_quantity_on_hand"}},
        // Dimension tables
        {"customer",
         {"c_customer_sk", "c_customer_id", "c_current_cdemo_sk",
          "c_current_hdemo_sk", "c_current_addr_sk", "c_first_shipto_date_sk",
          "c_first_sales_date_sk", "c_salutation", "c_first_name", "c_last_name",
          "c_preferred_cust_flag", "c_birth_day", "c_birth_month", "c_birth_year",
          "c_birth_country", "c_login", "c_email_address",
          "c_last_review_date_sk"}},
        {"customer_address",
         {"ca_address_sk", "ca_address_id", "ca_street_number", "ca_street_name",
          "ca_street_type", "ca_suite_number", "ca_city", "ca_county", "ca_state",
          "ca_zip", "ca_country", "ca_gmt_offset", "ca_location_type"}},
        {"customer_demographics",
         {"cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status",
          "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count",
          "cd_dep_employed_count", "cd_dep_college_count"}},
        {"date_dim",
         {"d_date_sk", "d_date_id", "d_date", "d_month_seq", "d_week_seq",
          "d_quarter_seq", "d_year", "d_dow", "d_moy", "d_dom", "d_qoy",
          "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq", "d_day_name",
          "d_quarter_name", "d_holiday", "d_weekend", "d_following_holiday",
          "d_first_dom", "d_last_dom", "d_same_day_ly", "d_same_day_lq",
          "d_current_day", "d_current_week", "d_current_month",
          "d_current_quarter", "d_current_year"}},
        {"time_dim",
         {"t_time_sk", "t_time_id", "t_time", "t_hour", "t_minute", "t_second",
          "t_am_pm", "t_shift", "t_sub_shift", "t_meal_time"}},
        {"item",
         {"i_item_sk", "i_item_id", "i_rec_start_date", "i_rec_end_date",
          "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand_id",
          "i_brand", "i_class_id", "i_class", "i_category_id", "i_category",
          "i_manufact_id", "i_manufact", "i_size", "i_formulation", "i_color",
          "i_units", "i_container", "i_manager_id", "i_product_name"}},
        {"store",
         {"s_store_sk", "s_store_id", "s_rec_start_date", "s_rec_end_date",
          "s_closed_date_sk", "s_store_name", "s_number_employees",
          "s_floor_space", "s_hours", "s_manager", "s_market_id",
          "s_geography_class", "s_market_desc", "s_market_manager",
          "s_division_id", "s_division_name", "s_company_id", "s_company_name",
          "s_street_number", "s_street_name", "s_street_type", "s_suite_number",
          "s_city", "s_county", "s_state", "s_zip", "s_country", "s_gmt_offset",
          "s_tax_percentage"}},
        {"promotion",
         {"p_promo_sk", "p_promo_id", "p_start_date_sk", "p_end_date_sk",
          "p_item_sk", "p_cost", "p_response_target", "p_promo_name",
          "p_channel_dmail", "p_channel_email", "p_channel_catalog",
          "p_channel_tv", "p_channel_radio", "p_channel_press", "p_channel_event",
          "p_channel_demo", "p_channel_details", "p_purpose", "p_discount_active"}},
        {"household_demographics",
         {"hd_demo_sk", "hd_income_band_sk", "hd_buy_potential", "hd_dep_count",
          "hd_vehicle_count"}},
        {"warehouse",
         {"w_warehouse_sk", "w_warehouse_id", "w_warehouse_name",
          "w_warehouse_sq_ft", "w_street_number", "w_street_name",
          "w_street_type", "w_suite_number", "w_city", "w_county", "w_state",
          "w_zip", "w_country", "w_gmt_offset"}},
        {"ship_mode",
         {"sm_ship_mode_sk", "sm_ship_mode_id", "sm_type", "sm_code",
          "sm_carrier", "sm_contract"}},
        {"reason", {"r_reason_sk", "r_reason_id", "r_reason_desc"}},
        {"income_band", {"ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"}},
        {"call_center",
         {"cc_call_center_sk", "cc_call_center_id", "cc_rec_start_date",
          "cc_rec_end_date", "cc_closed_date_sk", "cc_open_date_sk", "cc_name",
          "cc_class", "cc_employees", "cc_sq_ft", "cc_hours", "cc_manager",
          "cc_mkt_id", "cc_mkt_class", "cc_mkt_desc", "cc_market_manager",
          "cc_division", "cc_division_name", "cc_company", "cc_company_name",
          "cc_street_number", "cc_street_name", "cc_street_type",
          "cc_suite_number", "cc_city", "cc_county", "cc_state", "cc_zip",
          "cc_country", "cc_gmt_offset", "cc_tax_percentage"}},
        {"catalog_page",
         {"cp_catalog_page_sk", "cp_catalog_page_id", "cp_start_date_sk",
          "cp_end_date_sk", "cp_department", "cp_catalog_number",
          "cp_catalog_page_number", "cp_description", "cp_type"}},
        {"web_page",
         {"wp_web_page_sk", "wp_web_page_id", "wp_rec_start_date",
          "wp_rec_end_date", "wp_creation_date_sk", "wp_access_date_sk",
          "wp_autogen_flag", "wp_customer_sk", "wp_url", "wp_type",
          "wp_char_count", "wp_link_count", "wp_image_count", "wp_max_ad_count"}},
        {"web_site",
         {"web_site_sk", "web_site_id", "web_rec_start_date", "web_rec_end_date",
          "web_name", "web_open_date_sk", "web_close_date_sk", "web_class",
          "web_manager", "web_mkt_id", "web_mkt_class", "web_mkt_desc",
          "web_market_manager", "web_company_id", "web_company_name",
          "web_street_number", "web_street_name", "web_street_type",
          "web_suite_number", "web_city", "web_county", "web_state", "web_zip",
          "web_country", "web_gmt_offset", "web_tax_percentage"}}};

} // namespace bytedance::bolt::exec::test
