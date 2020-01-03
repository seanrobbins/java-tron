package org.tron.core.actuator;

import static org.testng.Assert.fail;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.FileUtil;
import org.tron.core.capsule.utils.MarketUtils;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.Wallet;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.AssetIssueCapsule;
import org.tron.core.capsule.MarketAccountOrderCapsule;
import org.tron.core.capsule.MarketOrderCapsule;
import org.tron.core.capsule.MarketOrderIdListCapsule;
import org.tron.core.capsule.MarketPriceLinkedListCapsule;
import org.tron.core.capsule.TransactionResultCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.Manager;
import org.tron.core.exception.ContractExeException;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.exception.ItemNotFoundException;
import org.tron.core.store.MarketAccountStore;
import org.tron.core.store.MarketOrderStore;
import org.tron.core.store.MarketPairPriceToOrderStore;
import org.tron.core.store.MarketPairToPriceStore;
import org.tron.core.store.MarketPriceStore;
import org.tron.protos.Protocol.AccountType;
import org.tron.protos.Protocol.MarketOrder.State;
import org.tron.protos.Protocol.MarketPrice;
import org.tron.protos.contract.AssetIssueContractOuterClass.AssetIssueContract;
import org.tron.protos.contract.MarketContract.MarketSellAssetContract;

@Slf4j

public class MarketSellAssetActuatorTest {

  private static final String dbPath = "output_MarketSellAsset_test";
  private static final String ACCOUNT_NAME_FIRST = "ownerF";
  private static final String OWNER_ADDRESS_FIRST;
  private static final String ACCOUNT_NAME_SECOND = "ownerS";
  private static final String OWNER_ADDRESS_SECOND;
  private static final String OWNER_ADDRESS_NOT_EXIST;
  private static final String OWNER_ADDRESS_INVALID = "aaaa";
  private static final String TOKEN_ID_ONE = String.valueOf(1L);
  private static final String TOKEN_ID_TWO = String.valueOf(2L);
  private static final String TRX = "_";
  private static TronApplicationContext context;
  private static Manager dbManager;

  static {
    Args.setParam(new String[]{"--output-directory", dbPath}, Constant.TEST_CONF);
    context = new TronApplicationContext(DefaultConfig.class);
    OWNER_ADDRESS_FIRST =
        Wallet.getAddressPreFixString() + "abd4b9367799eaa3197fecb144eb71de1e049abc";
    OWNER_ADDRESS_SECOND =
        Wallet.getAddressPreFixString() + "548794500882809695a8a687866e76d4271a1abc";
    OWNER_ADDRESS_NOT_EXIST =
        Wallet.getAddressPreFixString() + "548794500882809695a8a687866e06d4271a1c11";
  }

  /**
   * Init data.
   */
  @BeforeClass
  public static void init() {
    dbManager = context.getBean(Manager.class);
  }

  /**
   * Release resources.
   */
  @AfterClass
  public static void destroy() {
    Args.clearParam();
    context.destroy();
    if (FileUtil.deleteDir(new File(dbPath))) {
      logger.info("Release resources successful.");
    } else {
      logger.info("Release resources failure.");
    }
  }

  /**
   * create temp Capsule test need.
   */
  @Before
  public void initTest() {
    byte[] ownerAddressFirstBytes = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    byte[] ownerAddressSecondBytes = ByteArray.fromHexString(OWNER_ADDRESS_SECOND);

    AccountCapsule ownerAccountFirstCapsule =
        new AccountCapsule(
            ByteString.copyFromUtf8(ACCOUNT_NAME_FIRST),
            ByteString.copyFrom(ownerAddressFirstBytes),
            AccountType.Normal,
            10000_000_000L);
    AccountCapsule ownerAccountSecondCapsule =
        new AccountCapsule(
            ByteString.copyFromUtf8(ACCOUNT_NAME_SECOND),
            ByteString.copyFrom(ownerAddressSecondBytes),
            AccountType.Normal,
            20000_000_000L);

    // init account
    dbManager.getAccountStore()
        .put(ownerAccountFirstCapsule.getAddress().toByteArray(), ownerAccountFirstCapsule);
    dbManager.getAccountStore()
        .put(ownerAccountSecondCapsule.getAddress().toByteArray(), ownerAccountSecondCapsule);

//    InitAsset();

    dbManager.getDynamicPropertiesStore().saveLatestBlockHeaderTimestamp(1000000);
    dbManager.getDynamicPropertiesStore().saveLatestBlockHeaderNumber(10);
    dbManager.getDynamicPropertiesStore().saveNextMaintenanceTime(2000000);
  }


  private void InitAsset() {
    AssetIssueCapsule assetIssueCapsule1 = new AssetIssueCapsule(
        AssetIssueContract.newBuilder()
            .setName(ByteString.copyFrom("abc".getBytes()))
            .setId(TOKEN_ID_ONE)
            .build());

    AssetIssueCapsule assetIssueCapsule2 = new AssetIssueCapsule(
        AssetIssueContract.newBuilder()
            .setName(ByteString.copyFrom("def".getBytes()))
            .setId(TOKEN_ID_TWO)
            .build());
    dbManager.getAssetIssueV2Store().put(assetIssueCapsule1.createDbV2Key(), assetIssueCapsule1);
    dbManager.getAssetIssueV2Store().put(assetIssueCapsule2.createDbV2Key(), assetIssueCapsule2);
  }

  @After
  public void cleanDb() {
    byte[] ownerAddressFirstBytes = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    byte[] ownerAddressSecondBytes = ByteArray.fromHexString(OWNER_ADDRESS_SECOND);

    // delete order
    cleanMarketOrderByAccount(ownerAddressFirstBytes);
    cleanMarketOrderByAccount(ownerAddressSecondBytes);
    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();

    chainBaseManager.getMarketAccountStore().delete(ownerAddressFirstBytes);
    chainBaseManager.getMarketAccountStore().delete(ownerAddressSecondBytes);

    //delete priceList
    chainBaseManager.getMarketPairToPriceStore()
        .delete(MarketUtils.createPairKey(TOKEN_ID_ONE.getBytes(), TOKEN_ID_TWO.getBytes()));
    chainBaseManager.getMarketPairToPriceStore()
        .delete(MarketUtils.createPairKey(TOKEN_ID_TWO.getBytes(), TOKEN_ID_ONE.getBytes()));

    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();
    marketPriceStore.forEach(marketPriceCapsuleEntry -> {
      marketPriceStore.delete(marketPriceCapsuleEntry.getKey());
    });

    MarketPairToPriceStore pairToPriceStore = chainBaseManager
        .getMarketPairToPriceStore();
    pairToPriceStore.forEach(
        marketPriceLinkedListCapsuleEntry -> pairToPriceStore
            .delete(marketPriceLinkedListCapsuleEntry.getKey()));

    //delete orderList
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    pairPriceToOrderStore.forEach(
        marketOrderIdListCapsuleEntry -> pairPriceToOrderStore
            .delete(marketOrderIdListCapsuleEntry.getKey()));
  }

  private void cleanMarketOrderByAccount(byte[] accountAddress) {

    if (accountAddress == null || accountAddress.length == 0) {
      return;
    }

    MarketAccountOrderCapsule marketAccountOrderCapsule;
    try {
      marketAccountOrderCapsule = dbManager.getChainBaseManager()
          .getMarketAccountStore().get(accountAddress);
    } catch (ItemNotFoundException e) {
      return;
    }

    MarketOrderStore marketOrderStore = dbManager.getChainBaseManager().getMarketOrderStore();

    List<ByteString> orderIdList = marketAccountOrderCapsule.getOrdersList();
    orderIdList.forEach(
        orderId -> marketOrderStore.delete(orderId.toByteArray())
    );
  }

  private Any getContract(String address, String sellTokenId, long sellTokenQuantity,
      String buyTokenId, long buyTokenQuantity) {

    return Any.pack(
        MarketSellAssetContract.newBuilder()
            .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(address)))
            .setSellTokenId(ByteString.copyFrom(sellTokenId.getBytes()))
            .setSellTokenQuantity(sellTokenQuantity)
            .setBuyTokenId(ByteString.copyFrom(buyTokenId.getBytes()))
            .setBuyTokenQuantity(buyTokenQuantity)
            .build());
  }

  private Any getContract(String address, String sellTokenId, long sellTokenQuantity,
      String buyTokenId, long buyTokenQuantity, ByteString prePriceKey) {

    return Any.pack(
        MarketSellAssetContract.newBuilder()
            .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(address)))
            .setSellTokenId(ByteString.copyFrom(sellTokenId.getBytes()))
            .setSellTokenQuantity(sellTokenQuantity)
            .setBuyTokenId(ByteString.copyFrom(buyTokenId.getBytes()))
            .setBuyTokenQuantity(buyTokenQuantity)
            .setPrePriceKey(prePriceKey)
            .build());
  }

  //test case
  //
  // validate:
  // ownerAddress,token,Account,TokenQuantity
  // balance(fee) not enough,token not enough


  /**
   * use Invalid Address, result is failed, exception is "Invalid address".
   */
  @Test
  public void invalidOwnerAddress() {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();
    String sellTokenId = "123";
    long sellTokenQuant = 100000000L;
    String buyTokenId = "456";
    long buyTokenQuant = 200000000L;

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_INVALID, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    try {
      actuator.validate();
      actuator.execute(ret);
      fail("Invalid address");
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals("Invalid address", e.getMessage());
    } catch (ContractExeException e) {
      Assert.assertFalse(e instanceof ContractExeException);
    }
  }

  /**
   * Account not exist , result is failed, exception is "token quantity must greater than zero".
   */
  @Test
  public void notExistAccount() {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();
    String sellTokenId = "123";
    long sellTokenQuant = 100000000L;
    String buyTokenId = "456";
    long buyTokenQuant = 200000000L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_NOT_EXIST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertEquals(null, accountCapsule);

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_NOT_EXIST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    try {
      actuator.validate();
      actuator.execute(ret);
      fail("Account does not exist!");
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals("Account does not exist!", e.getMessage());
    } catch (ContractExeException e) {
      Assert.assertFalse(e instanceof ContractExeException);
    }
  }

  /**
   * use negative sell quantity, result is failed, exception is "sellTokenQuantity must greater than
   * 0!".
   */
  @Test
  public void invalidSellQuantity() {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();
    String sellTokenId = "123";
    long sellTokenQuant = -100000000L;
    String buyTokenId = "456";
    long buyTokenQuant = 200000000L;

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    try {
      actuator.validate();
      actuator.execute(ret);
      fail("token quantity must greater than zero");
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals("token quantity must greater than zero", e.getMessage());
    } catch (ContractExeException e) {
      Assert.assertFalse(e instanceof ContractExeException);
    }
  }


  /**
   * no Enough Balance For Selling TRX, result is failed, exception is "No enough balance !".
   */
  @Test
  public void noEnoughBalanceForSellingTRX() {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertEquals(10000_000000L, accountCapsule.getBalance());

    String sellTokenId = "_";
    //sellTokenQuant = balance - fee + 1
    long sellTokenQuant = accountCapsule.getBalance()
        - dbManager.getDynamicPropertiesStore().getMarketSellFee() + 1;
    String buyTokenId = "456";
    long buyTokenQuant = 200_000000L;

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    String errorMessage = "No enough balance !";
    try {
      actuator.validate();
      actuator.execute(ret);
      fail(errorMessage);
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals(errorMessage, e.getMessage());
    } catch (ContractExeException e) {
      Assert.assertFalse(e instanceof ContractExeException);
    }
  }


  /**
   * no Enough Balance For Selling Token, result is failed, exception is "No enough balance !".
   */
  @Test
  public void noEnoughBalanceForSellingToken() {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    //balance = fee - 1
    accountCapsule.setBalance(dbManager.getDynamicPropertiesStore().getMarketSellFee() - 1L);
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);

    String sellTokenId = "123";
    long sellTokenQuant = 100_000000L;
    String buyTokenId = "456";
    long buyTokenQuant = 200_000000L;

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    String errorMessage = "No enough balance !";
    try {
      actuator.validate();
      actuator.execute(ret);
      fail(errorMessage);
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals(errorMessage, e.getMessage());
    } catch (ContractExeException e) {
      Assert.assertFalse(e instanceof ContractExeException);
    }
  }


  /**
   * no sell Token Id, result is failed, exception is "No sellTokenID".
   */
  @Test
  public void noSellTokenID() {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    String sellTokenId = "123";
    long sellTokenQuant = 100_000000L;
    String buyTokenId = "456";
    long buyTokenQuant = 200_000000L;

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    String errorMessage = "No sellTokenID !";
    try {
      actuator.validate();
      actuator.execute(ret);
      fail(errorMessage);
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals(errorMessage, e.getMessage());
    } catch (ContractExeException e) {
      Assert.assertFalse(e instanceof ContractExeException);
    }
  }


  /**
   * SellToken balance is not enough, result is failed, exception is "No buyTokenID !".
   */
  @Test
  public void notEnoughSellToken() {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100_000000L;
    String buyTokenId = "456";
    long buyTokenQuant = 200_000000L;

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    String errorMessage = "SellToken balance is not enough !";
    try {
      actuator.validate();
      actuator.execute(ret);
      fail(errorMessage);
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals(errorMessage, e.getMessage());
    } catch (ContractExeException e) {
      Assert.assertFalse(e instanceof ContractExeException);
    }
  }

  /**
   * No buyTokenID, result is failed, exception is "No buyTokenID".
   */
  @Test
  public void noBuyTokenID() {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100_000000L;
    String buyTokenId = "456";
    long buyTokenQuant = 200_000000L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    String errorMessage = "No buyTokenID !";
    try {
      actuator.validate();
      fail(errorMessage);
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }


  /**
   * validate Success without position, result is Success .
   */
  @Test
  public void validateSuccessWithoutPosition() throws Exception {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = "_";
    long buyTokenQuant = 300L;

    for (int i = 0; i < 10; i++) {
      addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
          200L + i, OWNER_ADDRESS_FIRST);
    }

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    try {
      actuator.validate();
    } catch (ContractValidateException e) {
      fail("validateSuccess error");
    }
  }


  /**
   * without position, time out
   */
  @Test
  public void withoutPositionTimeOut() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    // TOKEN_ID_ONE has the same value as TOKEN_ID_ONE
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 300L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    // Initialize the order book
    //MAX_SEARCH_NUM = 10
    for (int i = 0; i < 11; i++) {
      addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
          200L + i, OWNER_ADDRESS_FIRST);
    }

    //the final price order should be : order_1, order_current, order_2
    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    String errorMessage = "Maximum number of queries exceeded，10";
    try {
      actuator.validate();
      fail(errorMessage);
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  /**
   * position price error ,prePriceKey not exists
   */
  @Test
  public void prePriceKeyNotExists() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    // TOKEN_ID_ONE has the same value as TOKEN_ID_ONE
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 300L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    ByteString prePreKey = ByteString.copyFromUtf8("not exists");
    //the final price order should be : order_1, order_current, order_2
    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant, prePreKey));

    String errorMessage = "prePriceKey not exists";
    try {
      actuator.validate();
      fail(errorMessage);
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  private void prepareAccount(String sellTokenId, String buyTokenId,
      long sellTokenQuant, long buyTokenQuant, byte[] ownerAddress) {
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);
  }

  /**
   * position price error ,pre price should be less than current price
   */
  @Test
  public void prePriceError() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    // TOKEN_ID_ONE has the same value as TOKEN_ID_ONE
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 300L;
    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);

    prepareAccount(sellTokenId, buyTokenId, sellTokenQuant, buyTokenQuant, ownerAddress);

    //pre price should be less than current price
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        400L, OWNER_ADDRESS_FIRST);

    byte[] prePreKey = MarketUtils
        .createPairPriceKey(TOKEN_ID_ONE.getBytes(), TOKEN_ID_TWO.getBytes(), 100L, 400L);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant,
        ByteString.copyFrom(prePreKey)));

    String errorMessage = "pre price should be less than current price";
    try {
      actuator.validate();
      fail(errorMessage);
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  /**
   * position  time out
   */
  @Test
  public void positionTimeOut() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    // TOKEN_ID_ONE has the same value as TOKEN_ID_ONE
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 300L;
    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);

    prepareAccount(sellTokenId, buyTokenId, sellTokenQuant, buyTokenQuant, ownerAddress);

    //MAX_SEARCH_NUM = 10
    for (int i = 0; i < 20; i++) {
      byte[] prePriceKey = null;
      if (i != 0) {
        prePriceKey = MarketUtils
            .createPairPriceKey(TOKEN_ID_ONE.getBytes(), TOKEN_ID_TWO.getBytes(), 100L,
                200L + i - 1);
      }

      addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
          200L + i, OWNER_ADDRESS_FIRST, prePriceKey);
    }

    byte[] prePriceKey = MarketUtils
        .createPairPriceKey(TOKEN_ID_ONE.getBytes(), TOKEN_ID_TWO.getBytes(), 100L, 208L);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant,
        ByteString.copyFrom(prePriceKey)));

    String errorMessage = "Maximum number of queries exceeded，10";
    try {
      actuator.validate();
      fail(errorMessage);
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  /**
   * validate Success with position, result is Success .
   */
  @Test
  public void validateSuccessWithPosition() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    // TOKEN_ID_ONE has the same value as TOKEN_ID_ONE
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 300L;
    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);

    prepareAccount(sellTokenId, buyTokenId, sellTokenQuant, buyTokenQuant, ownerAddress);

    //MAX_SEARCH_NUM = 10
    for (int i = 0; i < 20; i++) {
      byte[] prePriceKey = null;
      if (i != 0) {
        prePriceKey = MarketUtils
            .createPairPriceKey(TOKEN_ID_ONE.getBytes(), TOKEN_ID_TWO.getBytes(), 100L,
                200L + i - 1);
      }

      addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
          200L + i, OWNER_ADDRESS_FIRST, prePriceKey);
    }

    byte[] prePriceKey = MarketUtils
        .createPairPriceKey(TOKEN_ID_ONE.getBytes(), TOKEN_ID_TWO.getBytes(), 100L, 210L);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant,
        ByteString.copyFrom(prePriceKey)));

    try {
      actuator.validate();
    } catch (ContractValidateException e) {
      fail("validateSuccess error");
    }
  }

  private void addOrder(String sellTokenId, long sellTokenQuant,
      String buyTokenId, long buyTokenQuant, String ownAddress) throws Exception {
    addOrder(sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant, ownAddress, null);
  }

  private void addOrder(String sellTokenId, long sellTokenQuant,
      String buyTokenId, long buyTokenQuant, String ownAddress, byte[] prePreKey) throws Exception {

    byte[] ownerAddress = ByteArray.fromHexString(ownAddress);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    if (prePreKey == null || prePreKey.length == 0) {
      actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
          ownAddress, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));
    } else {
      actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
          ownAddress, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant,
          ByteString.copyFrom(prePreKey)));
    }

    TransactionResultCapsule ret = new TransactionResultCapsule();
    actuator.validate();
    actuator.execute(ret);
  }

  // execute: combination
  // Trading object：
  //    abc to def
  //    abc to trx
  //    trx to abc
  // Scenes：
  //    no buy orders before,add first sell order
  //    no buy orders before，add multiple sell orders, need to maintain the correct order
  //    no buy orders before，add multiple sell orders, need to maintain the correct order,
  //      same price
  //    has buy orders before，add first sell order，not match
  //    has buy orders and sell orders before，add sell order，not match,
  //      need to maintain the correct order

  //    all match with 2 existing same price buy orders and complete all 3 orders
  //    part match with 2 existing buy orders and complete the maker,
  //        left enough
  //        left not enough and return left（Accuracy problem）
  //    part match with 2 existing buy orders and complete the taker,
  //        left enough
  //        left not enough and return left（Accuracy problem）（not exist)

  /**
   * no buy orders before,add first sell order,selling TRX and buying token
   */
  @Test
  public void noBuyAddFirstSellOrder1() throws Exception {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    String sellTokenId = "_";
    long sellTokenQuant = 100_000000L;
    String buyTokenId = TOKEN_ID_ONE;
    long buyTokenQuant = 200_000000L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    long balanceBefore = accountCapsule.getBalance();

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    actuator.validate();
    actuator.execute(ret);

    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check balance
    accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertEquals(balanceBefore, sellTokenQuant
        + dbManager.getDynamicPropertiesStore().getMarketSellFee() + accountCapsule.getBalance());

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 1);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(0);

    //check order
    MarketOrderCapsule orderCapsule = orderStore.get(orderId.toByteArray());
    Assert.assertEquals(orderCapsule.getSellTokenQuantityRemain(), sellTokenQuant);

    //check pairToPrice
    byte[] marketPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule priceListCapsule = pairToPriceStore.get(marketPair);
    Assert.assertEquals(priceListCapsule.getPriceSize(marketPriceStore), 1);
    Assert.assertTrue(Arrays.equals(priceListCapsule.getSellTokenId(), sellTokenId.getBytes()));
    Assert.assertTrue(Arrays.equals(priceListCapsule.getBuyTokenId(), buyTokenId.getBytes()));

    MarketPrice marketPrice = priceListCapsule.getBestPrice();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), sellTokenQuant);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), buyTokenQuant);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        priceListCapsule.getSellTokenId(), priceListCapsule.getBuyTokenId(),
        marketPrice.getSellTokenQuantity(), marketPrice.getBuyTokenQuantity());
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .get(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule.getOrderSize(orderStore), 1);
    Assert.assertArrayEquals(orderIdListCapsule.getHead(),
        orderId.toByteArray());
  }

  /**
   * no buy orders before,add first sell order,selling Token and buying TRX
   */
  @Test
  public void noBuyAddFirstSellOrder2() throws Exception {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100_000000L;
    String buyTokenId = "_";
    long buyTokenQuant = 200_000000L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    long balanceBefore = accountCapsule.getBalance();
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    actuator.validate();
    actuator.execute(ret);

    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check balance and token
    accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertEquals(balanceBefore,
        dbManager.getDynamicPropertiesStore().getMarketSellFee() + accountCapsule.getBalance());
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == 0L);

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 1);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(0);

    //check order
    MarketOrderCapsule orderCapsule = orderStore.get(orderId.toByteArray());
    Assert.assertEquals(orderCapsule.getSellTokenQuantityRemain(), sellTokenQuant);

    //check pairToPrice
    byte[] marketPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule priceListCapsule = pairToPriceStore.get(marketPair);
    Assert.assertEquals(priceListCapsule.getPriceSize(marketPriceStore), 1);
    Assert.assertTrue(Arrays.equals(priceListCapsule.getSellTokenId(), sellTokenId.getBytes()));
    Assert.assertTrue(Arrays.equals(priceListCapsule.getBuyTokenId(), buyTokenId.getBytes()));

    MarketPrice marketPrice = priceListCapsule.getBestPrice();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), sellTokenQuant);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), buyTokenQuant);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        priceListCapsule.getSellTokenId(), priceListCapsule.getBuyTokenId(),
        marketPrice.getSellTokenQuantity(), marketPrice.getBuyTokenQuantity());
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .get(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule.getOrderSize(orderStore), 1);
    Assert.assertArrayEquals(orderIdListCapsule.getHead(),
        orderId.toByteArray());
  }


  /**
   * no buy orders before,add first sell order,selling Token and buying token
   */
  @Test
  public void noBuyAddFirstSellOrder3() throws Exception {
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100_000000L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 200_000000L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    long balanceBefore = accountCapsule.getBalance();
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    actuator.validate();
    actuator.execute(ret);

    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check balance and token
    accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertEquals(balanceBefore,
        dbManager.getDynamicPropertiesStore().getMarketSellFee() + accountCapsule.getBalance());
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == 0L);

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 1);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(0);

    //check order
    MarketOrderCapsule orderCapsule = orderStore.get(orderId.toByteArray());
    Assert.assertEquals(orderCapsule.getSellTokenQuantityRemain(), sellTokenQuant);

    //check pairToPrice
    byte[] marketPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule priceListCapsule = pairToPriceStore.get(marketPair);
    Assert.assertEquals(priceListCapsule.getPriceSize(marketPriceStore), 1);
    Assert.assertTrue(Arrays.equals(priceListCapsule.getSellTokenId(), sellTokenId.getBytes()));
    Assert.assertTrue(Arrays.equals(priceListCapsule.getBuyTokenId(), buyTokenId.getBytes()));

    MarketPrice marketPrice = priceListCapsule.getBestPrice();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), sellTokenQuant);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), buyTokenQuant);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        priceListCapsule.getSellTokenId(), priceListCapsule.getBuyTokenId(),
        marketPrice.getSellTokenQuantity(), marketPrice.getBuyTokenQuantity());
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .get(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule.getOrderSize(orderStore), 1);
    Assert.assertTrue(Arrays.equals(orderIdListCapsule.getHead(),
        orderId.toByteArray()));
  }


  /**
   * no buy orders before，add multiple sell orders,need to maintain the correct order
   */
  @Test
  public void noBuyAddMultiSellOrder1() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    // TOKEN_ID_ONE has the same value as TOKEN_ID_ONE
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 300L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    // Initialize the order book

    //add three order(sell id_1  and buy id_2) with different price by the same account
    //TOKEN_ID_ONE is twice as expensive as TOKEN_ID_TWO
    //order_1
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        200L, OWNER_ADDRESS_FIRST);
    //order_2
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        400L, OWNER_ADDRESS_FIRST);

    //the final price order should be : order_1, order_current, order_2
    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();
    actuator.validate();
    actuator.execute(ret);

    //get storeDB instance
    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 3);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(2);

    //check pairToPrice
    byte[] marketPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule priceListCapsule = pairToPriceStore.get(marketPair);
    Assert.assertEquals(priceListCapsule.getPriceSize(marketPriceStore), 3);

    //This order should be second one
    MarketPrice marketPrice = priceListCapsule.getPriceByIndex(1, marketPriceStore).getInstance();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), sellTokenQuant);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), buyTokenQuant);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        priceListCapsule.getSellTokenId(), priceListCapsule.getBuyTokenId(),
        marketPrice.getSellTokenQuantity(), marketPrice.getBuyTokenQuantity());
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .get(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule.getOrderSize(orderStore), 1);
    Assert.assertTrue(Arrays.equals(orderIdListCapsule.getHead(),
        orderId.toByteArray()));
  }


  /**
   * no buy orders before，add multiple sell orders,need to maintain the correct order,same price
   */
  @Test
  public void noBuyAddMultiSellOrderSamePrice1() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    // TOKEN_ID_ONE has the same value as TOKEN_ID_ONE
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 300L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    // Initialize the order book

    //add three order(sell id_1  and buy id_2) with different price by the same account
    //TOKEN_ID_ONE is twice as expensive as TOKEN_ID_TWO
    //order_1
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        200L, OWNER_ADDRESS_FIRST);
    //order_2
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        300L, OWNER_ADDRESS_FIRST);

    //the final price order should be : order_1, order_current, order_2
    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();
    actuator.validate();
    actuator.execute(ret);

    //get storeDB instance
    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 3);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(2);

    //check pairToPrice
    byte[] marketPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule priceListCapsule = pairToPriceStore.get(marketPair);
    Assert.assertEquals(priceListCapsule.getPriceSize(marketPriceStore), 2);

    //This order should be second one
    MarketPrice marketPrice = priceListCapsule.getPriceByIndex(1, marketPriceStore).getInstance();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), sellTokenQuant);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), buyTokenQuant);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        priceListCapsule.getSellTokenId(), priceListCapsule.getBuyTokenId(),
        marketPrice.getSellTokenQuantity(), marketPrice.getBuyTokenQuantity());
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .get(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule.getOrderSize(orderStore), 2);
    Assert
        .assertArrayEquals(orderIdListCapsule.getOrderByIndex(1, orderStore).getID().toByteArray(),
            orderId.toByteArray());
  }


  /**
   * has buy orders before，add first sell order，not match
   */
  @Test
  public void hasBuyAddFirstSellOrderNotMatch1() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //TOKEN_ID_ONE has the same value as TOKEN_ID_ONE
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 100L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    // Initialize the order book
    //add three order with different price by the same account

    //TOKEN_ID_TWO is twice as expensive as TOKEN_ID_ONE
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        200L, OWNER_ADDRESS_FIRST);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        300L, OWNER_ADDRESS_FIRST);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        400L, OWNER_ADDRESS_FIRST);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();
    actuator.validate();
    actuator.execute(ret);

    //get storeDB instance
    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check balance and token
    accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == 0L);

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 4);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(3);

    //check order
    MarketOrderCapsule orderCapsule = orderStore.get(orderId.toByteArray());
    Assert.assertEquals(orderCapsule.getSellTokenQuantityRemain(), sellTokenQuant);

    //check pairToPrice
    byte[] marketPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule priceListCapsule = pairToPriceStore.get(marketPair);
    Assert.assertEquals(priceListCapsule.getPriceSize(marketPriceStore), 1);
    Assert.assertTrue(Arrays.equals(priceListCapsule.getSellTokenId(), sellTokenId.getBytes()));
    Assert.assertTrue(Arrays.equals(priceListCapsule.getBuyTokenId(), buyTokenId.getBytes()));

    MarketPrice marketPrice = priceListCapsule.getBestPrice();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), sellTokenQuant);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), buyTokenQuant);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        priceListCapsule.getSellTokenId(), priceListCapsule.getBuyTokenId(),
        marketPrice.getSellTokenQuantity(), marketPrice.getBuyTokenQuantity());
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .get(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule.getOrderSize(orderStore), 1);
    Assert.assertArrayEquals(orderIdListCapsule.getHead(),
        orderId.toByteArray());
  }


  /**
   * has buy orders and sell orders before，add sell order ，not match,need to maintain the correct
   * order
   */
  @Test
  public void hasBuySellAddSellOrderNotMatch1() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    // TOKEN_ID_ONE has the same value as TOKEN_ID_ONE
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 100L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 100L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    // Initialize the order book

    //add three order(sell id_2 and buy id_1) with different price by the same account
    //TOKEN_ID_TWO is twice as expensive as TOKEN_ID_ONE
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        200L, OWNER_ADDRESS_FIRST);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        400L, OWNER_ADDRESS_FIRST);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        300L, OWNER_ADDRESS_FIRST);

    //add three order(sell id_1  and buy id_2)
    //TOKEN_ID_ONE is twice as expensive as TOKEN_ID_TWO
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        200L, OWNER_ADDRESS_FIRST);
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        300L, OWNER_ADDRESS_FIRST);
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        400L, OWNER_ADDRESS_FIRST);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();
    actuator.validate();
    actuator.execute(ret);

    //get storeDB instance
    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check balance and token
    accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == 0L);

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 7);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(6);

    //check order
    MarketOrderCapsule orderCapsule = orderStore.get(orderId.toByteArray());
    Assert.assertEquals(orderCapsule.getSellTokenQuantityRemain(), sellTokenQuant);

    //check pairToPrice
    byte[] marketPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule priceListCapsule = pairToPriceStore.get(marketPair);
    Assert.assertEquals(priceListCapsule.getPriceSize(marketPriceStore), 4);
    Assert.assertArrayEquals(priceListCapsule.getSellTokenId(), sellTokenId.getBytes());
    Assert.assertArrayEquals(priceListCapsule.getBuyTokenId(), buyTokenId.getBytes());

    MarketPrice marketPrice = priceListCapsule.getBestPrice();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), sellTokenQuant);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), buyTokenQuant);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        priceListCapsule.getSellTokenId(), priceListCapsule.getBuyTokenId(),
        marketPrice.getSellTokenQuantity(), marketPrice.getBuyTokenQuantity());
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .get(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule.getOrderSize(orderStore), 1);
    Assert
        .assertArrayEquals(orderIdListCapsule.getOrderByIndex(0, orderStore).getID().toByteArray(),
            orderId.toByteArray());
  }

  /**
   * all match with 2 existing same price buy orders and complete this order
   */
  @Test
  public void matchAll2SamePriceBuyOrders1() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 400L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 200L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    // Initialize the order book

    //add three order(sell id_2 and buy id_1) with different price by the same account
    //TOKEN_ID_TWO is twice as expensive as TOKEN_ID_ONE
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        200L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        200L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        300L, OWNER_ADDRESS_SECOND);

    //add three order(sell id_1  and buy id_2)
    //TOKEN_ID_ONE is twice as expensive as TOKEN_ID_TWO
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        200L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        300L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_ONE, 100L, TOKEN_ID_TWO,
        400L, OWNER_ADDRESS_SECOND);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();
    actuator.validate();
    actuator.execute(ret);

    //get storeDB instance
    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check balance and token
    accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == 0L);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(buyTokenId) == 200L);

    byte[] makerAddress = ByteArray.fromHexString(OWNER_ADDRESS_SECOND);
    AccountCapsule makerAccountCapsule = dbManager.getAccountStore().get(makerAddress);
    Assert.assertTrue(makerAccountCapsule.getAssetMapV2().get(sellTokenId) == 400L);

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 1);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(0);

    MarketAccountOrderCapsule makerAccountOrderCapsule = marketAccountStore.get(makerAddress);
    Assert.assertEquals(makerAccountOrderCapsule.getCount(), 6);
    ByteString makerOrderId1 = makerAccountOrderCapsule.getOrdersList().get(0);
    ByteString makerOrderId2 = makerAccountOrderCapsule.getOrdersList().get(1);

    //check order
    MarketOrderCapsule orderCapsule = orderStore.get(orderId.toByteArray());
    Assert.assertEquals(orderCapsule.getSellTokenQuantityRemain(), 0L);
    Assert.assertEquals(orderCapsule.getSt(), State.INACTIVE);

    MarketOrderCapsule makerOrderCapsule1 = orderStore.get(makerOrderId1.toByteArray());
    Assert.assertEquals(makerOrderCapsule1.getSellTokenQuantityRemain(), 0L);
    Assert.assertEquals(makerOrderCapsule1.getSt(), State.INACTIVE);

    MarketOrderCapsule makerOrderCapsule2 = orderStore.get(makerOrderId2.toByteArray());
    Assert.assertEquals(makerOrderCapsule2.getSellTokenQuantityRemain(), 0L);
    Assert.assertEquals(makerOrderCapsule2.getSt(), State.INACTIVE);

    //check pairToPrice
    byte[] takerPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule takerPriceListCapsule = pairToPriceStore.get(takerPair);
    Assert.assertEquals(takerPriceListCapsule.getPriceSize(marketPriceStore), 3);
    MarketPrice takerPrice = takerPriceListCapsule.getBestPrice();
    Assert.assertEquals(takerPrice.getSellTokenQuantity(), 100L);
    Assert.assertEquals(takerPrice.getBuyTokenQuantity(), 200L);

    byte[] makerPair = MarketUtils.createPairKey(TOKEN_ID_TWO.getBytes(), TOKEN_ID_ONE.getBytes());
    MarketPriceLinkedListCapsule makerPriceListCapsule = pairToPriceStore.get(makerPair);
    Assert.assertEquals(makerPriceListCapsule.getPriceSize(marketPriceStore), 1);
    MarketPrice marketPrice = makerPriceListCapsule.getBestPrice();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), 100L);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), 300L);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        TOKEN_ID_TWO.getBytes(), TOKEN_ID_ONE.getBytes(),
        100L, 200L);
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .getUnchecked(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule, null);

    pairPriceKey = MarketUtils.createPairPriceKey(
        TOKEN_ID_ONE.getBytes(), TOKEN_ID_TWO.getBytes(),
        400L, 200L);
    orderIdListCapsule = pairPriceToOrderStore
        .getUnchecked(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule, null);
  }

  /**
   * match with 2 existing buy orders and complete the maker,
   */
  @Test
  public void partMatchMakerBuyOrders1() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 800L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 200L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    // Initialize the order book

    //add three order(sell id_2 and buy id_1) with different price by the same account
    //TOKEN_ID_TWO is twice as expensive as TOKEN_ID_ONE
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        200L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        300L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        500L, OWNER_ADDRESS_SECOND);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();
    actuator.validate();
    actuator.execute(ret);

    //get storeDB instance
    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check balance and token
    accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == 0L);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(buyTokenId) == 200L);

    byte[] makerAddress = ByteArray.fromHexString(OWNER_ADDRESS_SECOND);
    AccountCapsule makerAccountCapsule = dbManager.getAccountStore().get(makerAddress);
    Assert.assertTrue(makerAccountCapsule.getAssetMapV2().get(sellTokenId) == 500L);

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 1);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(0);

    MarketAccountOrderCapsule makerAccountOrderCapsule = marketAccountStore.get(makerAddress);
    Assert.assertEquals(makerAccountOrderCapsule.getCount(), 3);
    ByteString makerOrderId1 = makerAccountOrderCapsule.getOrdersList().get(0);
    ByteString makerOrderId2 = makerAccountOrderCapsule.getOrdersList().get(1);

    //check order
    MarketOrderCapsule orderCapsule = orderStore.get(orderId.toByteArray());
    Assert.assertEquals(orderCapsule.getSellTokenQuantityRemain(), 300L);
    Assert.assertEquals(orderCapsule.getSt(), State.ACTIVE);

    MarketOrderCapsule makerOrderCapsule1 = orderStore.get(makerOrderId1.toByteArray());
    Assert.assertEquals(makerOrderCapsule1.getSellTokenQuantityRemain(), 0L);
    Assert.assertEquals(makerOrderCapsule1.getSt(), State.INACTIVE);

    MarketOrderCapsule makerOrderCapsule2 = orderStore.get(makerOrderId2.toByteArray());
    Assert.assertEquals(makerOrderCapsule2.getSellTokenQuantityRemain(), 0L);
    Assert.assertEquals(makerOrderCapsule2.getSt(), State.INACTIVE);

    //check pairToPrice
    byte[] takerPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule takerPriceListCapsule = pairToPriceStore.get(takerPair);
    Assert.assertEquals(takerPriceListCapsule.getPriceSize(marketPriceStore), 1);
    MarketPrice takerPrice = takerPriceListCapsule.getBestPrice();
    Assert.assertEquals(takerPrice.getSellTokenQuantity(), 800L);
    Assert.assertEquals(takerPrice.getBuyTokenQuantity(), 200L);

    byte[] makerPair = MarketUtils.createPairKey(TOKEN_ID_TWO.getBytes(), TOKEN_ID_ONE.getBytes());
    MarketPriceLinkedListCapsule makerPriceListCapsule = pairToPriceStore.get(makerPair);
    Assert.assertEquals(makerPriceListCapsule.getPriceSize(marketPriceStore), 1);
    MarketPrice marketPrice = makerPriceListCapsule.getBestPrice();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), 100L);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), 500L);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        TOKEN_ID_TWO.getBytes(), TOKEN_ID_ONE.getBytes(),
        100L, 200L);
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .getUnchecked(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule, null);

    pairPriceKey = MarketUtils.createPairPriceKey(
        TOKEN_ID_TWO.getBytes(), TOKEN_ID_ONE.getBytes(),
        100L, 300L);
    orderIdListCapsule = pairPriceToOrderStore
        .getUnchecked(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule, null);
  }

  /**
   * match with 2 existing buy orders and complete this order
   */
  @Test
  public void partMatchTakerBuyOrders1() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 800L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 200L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    // Initialize the order book

    //add three order(sell id_2 and buy id_1) with different price by the same account
    //TOKEN_ID_TWO is twice as expensive as TOKEN_ID_ONE
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        200L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_TWO, 200L, TOKEN_ID_ONE,
        800L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        500L, OWNER_ADDRESS_SECOND);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();
    actuator.validate();
    actuator.execute(ret);

    //get storeDB instance
    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check balance and token
    accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == 0L);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(buyTokenId) == 250L);

    byte[] makerAddress = ByteArray.fromHexString(OWNER_ADDRESS_SECOND);
    AccountCapsule makerAccountCapsule = dbManager.getAccountStore().get(makerAddress);
    Assert.assertTrue(makerAccountCapsule.getAssetMapV2().get(sellTokenId) == 800L);

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 1);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(0);

    MarketAccountOrderCapsule makerAccountOrderCapsule = marketAccountStore.get(makerAddress);
    Assert.assertEquals(makerAccountOrderCapsule.getCount(), 3);
    ByteString makerOrderId1 = makerAccountOrderCapsule.getOrdersList().get(0);
    ByteString makerOrderId2 = makerAccountOrderCapsule.getOrdersList().get(1);

    //check order
    MarketOrderCapsule orderCapsule = orderStore.get(orderId.toByteArray());
    Assert.assertEquals(orderCapsule.getSellTokenQuantityRemain(), 0L);
    Assert.assertEquals(orderCapsule.getSt(), State.INACTIVE);

    MarketOrderCapsule makerOrderCapsule1 = orderStore.get(makerOrderId1.toByteArray());
    Assert.assertEquals(makerOrderCapsule1.getSellTokenQuantityRemain(), 0L);
    Assert.assertEquals(makerOrderCapsule1.getSt(), State.INACTIVE);

    MarketOrderCapsule makerOrderCapsule2 = orderStore.get(makerOrderId2.toByteArray());
    Assert.assertEquals(makerOrderCapsule2.getSellTokenQuantityRemain(), 50L);
    Assert.assertEquals(makerOrderCapsule2.getSt(), State.ACTIVE);

    //check pairToPrice
    byte[] takerPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule takerPriceListCapsule = pairToPriceStore.getUnchecked(takerPair);
    Assert.assertEquals(takerPriceListCapsule, null);

    byte[] makerPair = MarketUtils.createPairKey(TOKEN_ID_TWO.getBytes(), TOKEN_ID_ONE.getBytes());
    MarketPriceLinkedListCapsule makerPriceListCapsule = pairToPriceStore.get(makerPair);
    Assert.assertEquals(makerPriceListCapsule.getPriceSize(marketPriceStore), 2);
    MarketPrice marketPrice = makerPriceListCapsule.getBestPrice();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), 200L);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), 800L);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        TOKEN_ID_TWO.getBytes(), TOKEN_ID_ONE.getBytes(),
        100L, 200L);
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .getUnchecked(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule, null);

    pairPriceKey = MarketUtils.createPairPriceKey(
        TOKEN_ID_ONE.getBytes(), TOKEN_ID_TWO.getBytes(),
        800L, 200L);
    orderIdListCapsule = pairPriceToOrderStore
        .getUnchecked(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule, null);
  }


  /**
   * match with 2 existing buy orders and complete the maker, taker left not enough and return
   * left（Accuracy problem）
   */
  @Test
  public void partMatchMakerLeftNotEnoughBuyOrders1() throws Exception {

    // Initialize the order
    dbManager.getDynamicPropertiesStore().saveAllowSameTokenName(1);
    InitAsset();

    //(sell id_1  and buy id_2)
    String sellTokenId = TOKEN_ID_ONE;
    long sellTokenQuant = 201L;
    String buyTokenId = TOKEN_ID_TWO;
    long buyTokenQuant = 100L;

    byte[] ownerAddress = ByteArray.fromHexString(OWNER_ADDRESS_FIRST);
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    accountCapsule.addAssetAmountV2(sellTokenId.getBytes(), sellTokenQuant,
        dbManager.getDynamicPropertiesStore(), dbManager.getAssetIssueStore());
    dbManager.getAccountStore().put(ownerAddress, accountCapsule);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == sellTokenQuant);

    // Initialize the order book

    //add three order(sell id_2 and buy id_1) with different price by the same account
    //TOKEN_ID_TWO is twice as expensive as TOKEN_ID_ONE
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        200L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        200L, OWNER_ADDRESS_SECOND);
    addOrder(TOKEN_ID_TWO, 100L, TOKEN_ID_ONE,
        500L, OWNER_ADDRESS_SECOND);

    // do process
    MarketSellAssetActuator actuator = new MarketSellAssetActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager()).setAny(getContract(
        OWNER_ADDRESS_FIRST, sellTokenId, sellTokenQuant, buyTokenId, buyTokenQuant));

    TransactionResultCapsule ret = new TransactionResultCapsule();
    actuator.validate();
    actuator.execute(ret);

    //get storeDB instance
    ChainBaseManager chainBaseManager = dbManager.getChainBaseManager();
    MarketAccountStore marketAccountStore = chainBaseManager.getMarketAccountStore();
    MarketOrderStore orderStore = chainBaseManager.getMarketOrderStore();
    MarketPairToPriceStore pairToPriceStore = chainBaseManager.getMarketPairToPriceStore();
    MarketPairPriceToOrderStore pairPriceToOrderStore = chainBaseManager
        .getMarketPairPriceToOrderStore();
    MarketPriceStore marketPriceStore = chainBaseManager.getMarketPriceStore();

    //check balance and token
    accountCapsule = dbManager.getAccountStore().get(ownerAddress);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(sellTokenId) == 1L);
    Assert.assertTrue(accountCapsule.getAssetMapV2().get(buyTokenId) == 100L);

    byte[] makerAddress = ByteArray.fromHexString(OWNER_ADDRESS_SECOND);
    AccountCapsule makerAccountCapsule = dbManager.getAccountStore().get(makerAddress);
    Assert.assertTrue(makerAccountCapsule.getAssetMapV2().get(sellTokenId) == 200L);

    //check accountOrder
    MarketAccountOrderCapsule accountOrderCapsule = marketAccountStore.get(ownerAddress);
    Assert.assertEquals(accountOrderCapsule.getCount(), 1);
    ByteString orderId = accountOrderCapsule.getOrdersList().get(0);

    MarketAccountOrderCapsule makerAccountOrderCapsule = marketAccountStore.get(makerAddress);
    Assert.assertEquals(makerAccountOrderCapsule.getCount(), 3);
    ByteString makerOrderId1 = makerAccountOrderCapsule.getOrdersList().get(0);
    ByteString makerOrderId2 = makerAccountOrderCapsule.getOrdersList().get(1);

    //check order
    MarketOrderCapsule orderCapsule = orderStore.get(orderId.toByteArray());
    Assert.assertEquals(orderCapsule.getSellTokenQuantityRemain(), 0L);
    Assert.assertEquals(orderCapsule.getSt(), State.INACTIVE);

    MarketOrderCapsule makerOrderCapsule1 = orderStore.get(makerOrderId1.toByteArray());
    Assert.assertEquals(makerOrderCapsule1.getSellTokenQuantityRemain(), 0L);
    Assert.assertEquals(makerOrderCapsule1.getSt(), State.INACTIVE);

    MarketOrderCapsule makerOrderCapsule2 = orderStore.get(makerOrderId2.toByteArray());
    Assert.assertEquals(makerOrderCapsule2.getSellTokenQuantityRemain(), 100L);
    Assert.assertEquals(makerOrderCapsule2.getSt(), State.ACTIVE);

    //check pairToPrice
    byte[] takerPair = MarketUtils.createPairKey(sellTokenId.getBytes(), buyTokenId.getBytes());
    MarketPriceLinkedListCapsule takerPriceListCapsule = pairToPriceStore.getUnchecked(takerPair);
    Assert.assertEquals(takerPriceListCapsule, null);

    byte[] makerPair = MarketUtils.createPairKey(TOKEN_ID_TWO.getBytes(), TOKEN_ID_ONE.getBytes());
    MarketPriceLinkedListCapsule makerPriceListCapsule = pairToPriceStore.get(makerPair);
    Assert.assertEquals(makerPriceListCapsule.getPriceSize(marketPriceStore), 2);
    MarketPrice marketPrice = makerPriceListCapsule.getBestPrice();
    Assert.assertEquals(marketPrice.getSellTokenQuantity(), 100L);
    Assert.assertEquals(marketPrice.getBuyTokenQuantity(), 200L);

    //check pairPriceToOrder
    byte[] pairPriceKey = MarketUtils.createPairPriceKey(
        TOKEN_ID_ONE.getBytes(), TOKEN_ID_TWO.getBytes(),
        201L, 100L);
    MarketOrderIdListCapsule orderIdListCapsule = pairPriceToOrderStore
        .getUnchecked(pairPriceKey);
    Assert.assertEquals(orderIdListCapsule, null);
  }

}