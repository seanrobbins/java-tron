/*
 * java-tron is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * java-tron is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.tron.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.tron.core.Constant.LAST_HASH;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.utils.ByteArray;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.protos.Protocal;
import org.tron.protos.Protocal.TXOutputs;
import org.tron.protos.core.TronBlock.Block;

public class BlockchainTest {

  private static final Logger logger = LoggerFactory.getLogger("Test");
  private static Blockchain blockchain;
  private static LevelDbDataSourceImpl mockBlockDB;
  private String testHash = "15f3988aa8d56eab3bfca45144bad77fc60acce50437a0a9d794a03a83c15c5e";
  private Supplier<Protocal.Block> newTestBlock = () -> {
    ByteString randomInputTXId = ByteString.copyFrom(ByteArray.fromString(UUID.randomUUID().toString()));
    Protocal.TXInput testTxInput = Protocal.TXInput.getDefaultInstance().toBuilder()
        .setTxID(randomInputTXId)
        .build();

    ByteString randomOutputTXId = ByteString.copyFrom(ByteArray.fromString(UUID.randomUUID().toString()));
    Protocal.TXOutput testTxOutput = Protocal.TXOutput.getDefaultInstance().toBuilder().setPubKeyHash(randomOutputTXId).build();

    ByteString randomTxId = ByteString.copyFrom(ByteArray.fromString(UUID.randomUUID().toString()));
    Protocal.Transaction testTransaction = Protocal.Transaction.newBuilder()
        .setId(randomTxId)
        .addVin(testTxInput)
        .addVout(testTxOutput)
        .build();

    Protocal.BlockHeader testBlockHeader = Protocal.BlockHeader.newBuilder().setHash(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build();

    return Protocal.Block.newBuilder().setBlockHeader(testBlockHeader).addTransactions(testTransaction).build();
  };

  /**
   * setup fo BlockchainTest.
   */
  @Before
  public void setup() throws IOException {
    mockBlockDB = Mockito.mock(LevelDbDataSourceImpl.class);
    Mockito.when(mockBlockDB.getData(LAST_HASH)).thenReturn(ByteArray.fromString(testHash));
    blockchain = new Blockchain(mockBlockDB);
  }

  @Test
  public void testBlockchainConstructorForNewBlockchain() {
    Mockito.when(mockBlockDB.getData(any())).thenReturn(ByteArray.fromString(null));
    blockchain = new Blockchain(mockBlockDB);
    assertTrue(blockchain.getLastHash() != null);
    assertEquals(blockchain.getCurrentHash(), blockchain.getLastHash());
    Mockito.verify(mockBlockDB).putData(LAST_HASH, blockchain.getLastHash());

    System.out.println(ByteArray.toHexString(blockchain.getLastHash()));
    logger.info("test blockchain: lastHash = {}, currentHash = {}",
        ByteArray.toHexString(blockchain.getLastHash()), ByteArray
            .toHexString(blockchain.getCurrentHash()));
  }

  @Test
  public void testBlockchainConstructorForExistingBlockchain()
      throws InvalidProtocolBufferException {
    byte[] testHash = ByteArray.fromHexString("83deec1d17cc829542c46b0a4fec523f62ee801d57897cb794af80a7c3d7e87b");
    Protocal.Block testBlock = newTestBlock.get();
    Mockito.when(mockBlockDB.getData(testHash)).thenReturn(testBlock.toByteArray());

    Mockito.when(mockBlockDB.getData(LAST_HASH)).thenReturn(testHash);
    blockchain = new Blockchain(mockBlockDB);
    assertEquals(testHash, blockchain.getLastHash());
    assertEquals(testHash, blockchain.getCurrentHash());
    Mockito.verify(mockBlockDB, Mockito.never()).putData(any(), eq(testHash));

    byte[] blockBytes = blockchain.getBlockDB().getData(blockchain.getLastHash());
    assertEquals(testBlock, Protocal.Block.parseFrom(blockBytes));

    logger.info("test blockchain: lastHash = {}, currentHash = {}",
        ByteArray.toHexString(blockchain.getLastHash()), ByteArray
            .toHexString(blockchain.getCurrentHash()));
  }

  @Test
  public void testFindTransaction() {
    Protocal.Block testBlock = newTestBlock.get();
    Protocal.Transaction testTransaction = testBlock.getTransactions(0);
    Mockito.when(mockBlockDB.getData(ByteArray.fromString(testHash))).thenReturn(testBlock.toByteArray());

    Protocal.Transaction result = blockchain.findTransaction(testTransaction.getId());

    assertEquals(testTransaction.getId(), result.getId());
    assertEquals(testTransaction.getVin(0).getTxID(), result.getVin(0).getTxID());
    assertEquals(testTransaction.getVout(0).getPubKeyHash(), result.getVout(0).getPubKeyHash());
    logger.info("{}", TransactionCapsule.toPrintString(result));
  }

  @Test
  public void testFindTransactionReturnsAnEmptyTransactionWhenTransactionIsNotFound() {
    ByteString randomTxId = ByteString.copyFrom(ByteArray.fromString(UUID.randomUUID().toString()));
    Block testBlock = Block.getDefaultInstance();
    Mockito.when(mockBlockDB.getData(ByteArray.fromString(testHash))).thenReturn(testBlock.toByteArray());

    Protocal.Transaction result = blockchain.findTransaction(randomTxId);

    assertTrue(result.getSerializedSize() == 0);
    assertTrue(result.getVinCount() == 0);
    assertTrue(result.getVoutCount() == 0);
    logger.info("{}", TransactionCapsule.toPrintString(result));
  }

  @Test
  public void testFindUtxo() {
    Protocal.Block testBlock = newTestBlock.get();
    Mockito.when(mockBlockDB.getData(ByteArray.fromString(testHash))).thenReturn(testBlock.toByteArray());

    long testAmount = 10;
    Wallet wallet = new Wallet();
    SpendableOutputs spendableOutputs = new SpendableOutputs();
    spendableOutputs.setAmount(testAmount + 1);
    spendableOutputs.setUnspentOutputs(new HashMap<>());
    UTXOSet mockUtxoSet = Mockito.mock(UTXOSet.class);
    Mockito.when(mockUtxoSet.findSpendableOutputs(wallet.getEcKey().getPubKey(), testAmount)
    ).thenReturn(spendableOutputs);
    Mockito.when(mockUtxoSet.getBlockchain()).thenReturn(blockchain);

    Protocal.Transaction transaction = TransactionCapsule.newTransaction(wallet,
        "fd0f3c8ab4877f0fd96cd156b0ad42ea7aa82c31", testAmount, mockUtxoSet);
    List<Protocal.Transaction> transactions = new ArrayList<>();
    transactions.add(transaction);
    blockchain.addBlock(BlockCapsule.newBlock(transactions, ByteString
        .copyFrom(new byte[]{1}), ByteString
        .copyFrom(new byte[]{1}), 1));
    HashMap<String, TXOutputs> utxo = blockchain.findUtxo();
  }

  @Test
  public void testAddBlockToChain() {
    ByteString parentHash = ByteString.copyFrom(ByteArray.fromHexString(
        "0304f784e4e7bae517bcab94c3e0c9214fb4ac7ff9d7d5a937d1f40031f87b85"));
    ByteString difficulty = ByteString.copyFrom(ByteArray.fromHexString("2001"));

    Wallet wallet = new Wallet();

    Protocal.Block block = BlockCapsule.newBlock(null, parentHash,
        difficulty, 0);
    LevelDbDataSourceImpl levelDbDataSource = new LevelDbDataSourceImpl(Constant.TEST,
        Constant.OUTPUT_DIR,
        "blockStore_test");
    levelDbDataSource.initDB();
    String lastHash = "lastHash";
    byte[] key = lastHash.getBytes();
    String value = "090383489592535";
    byte[] values = value.getBytes();
    levelDbDataSource.putData(key, values);

    blockchain.addBlock(block);
    levelDbDataSource.closeDB();
  }
}
