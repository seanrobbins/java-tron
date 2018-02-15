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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.tron.core.Constant.LAST_HASH;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.utils.ByteArray;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.protos.Protocal.Block;
import org.tron.protos.Protocal.BlockHeader;
import org.tron.protos.Protocal.TXInput;
import org.tron.protos.Protocal.TXOutput;
import org.tron.protos.Protocal.TXOutputs;
import org.tron.protos.Protocal.Transaction;

public class BlockchainTest {

  private static final Logger logger = LoggerFactory.getLogger("Test");
  private static Blockchain blockchain;
  private static LevelDbDataSourceImpl mockBlockDB;
  private String testGenesisBlockHash;
  private byte[] testGenesisBlockHashByteArray;

  private Block mockGenesisBlock(String hash) {
    return newTestBlock(hash, null);
  }

  private Block newTestBlock(String hash, String parentHash) {
    ByteString randomInputTXId = ByteString.copyFrom(ByteArray.fromString(UUID.randomUUID().toString()));
    TXInput testTxInput = TXInput.getDefaultInstance().toBuilder()
        .setTxID(randomInputTXId)
        .setVout(0L)
        .build();

    ByteString randomOutputTXId = ByteString.copyFrom(ByteArray.fromString(UUID.randomUUID().toString()));
    TXOutput testTxOutput = TXOutput.getDefaultInstance().toBuilder()
        .setPubKeyHash(randomOutputTXId)
        .setValue(10L)
        .build();

    ByteString randomTxId = ByteString.copyFrom(ByteArray.fromString(UUID.randomUUID().toString()));
    Transaction testTransaction = Transaction.newBuilder()
        .setId(randomTxId)
        .addVin(testTxInput)
        .addVout(testTxOutput)
        .build();

    BlockHeader testBlockHeader = BlockHeader
        .newBuilder()
        .setHash(ByteString.copyFromUtf8(hash))
        .setParentHash(Optional.ofNullable(parentHash).map(ByteString::copyFromUtf8).orElse(ByteString.EMPTY))
        .build();

    return Block.newBuilder().setBlockHeader(testBlockHeader).addTransactions(testTransaction).build();
  }

  /**
   * setup fo BlockchainTest.
   */
  @Before
  public void setup() throws IOException {
    testGenesisBlockHash = "15f3988aa8d56eab3bfca45144bad77fc60acce50437a0a9d794a03a83c15c5e";
    testGenesisBlockHashByteArray = ByteArray.fromHexString(testGenesisBlockHash);

    mockBlockDB = Mockito.mock(LevelDbDataSourceImpl.class);
    Mockito.when(mockBlockDB.getData(LAST_HASH)).thenReturn(testGenesisBlockHashByteArray);
    blockchain = new Blockchain(mockBlockDB);
  }

  @Test
  public void testBlockchainConstructorForNewBlockchain() {
    Mockito.when(mockBlockDB.getData(any())).thenReturn(ByteArray.fromString(null));

    blockchain = new Blockchain(mockBlockDB);
    assertTrue(blockchain.getLastHash() != null);
    assertEquals(blockchain.getCurrentHash(), blockchain.getLastHash());
    Mockito.verify(mockBlockDB).putData(LAST_HASH, blockchain.getLastHash());

    logger.info("test blockchain: lastHash = {}, currentHash = {}",
        ByteArray.toHexString(blockchain.getLastHash()), ByteArray
            .toHexString(blockchain.getCurrentHash()));
  }

  @Test
  public void testBlockchainConstructorForExistingBlockchain()
      throws InvalidProtocolBufferException {
    Block testBlock = mockGenesisBlock(testGenesisBlockHash);
    Mockito.when(mockBlockDB.getData(testGenesisBlockHashByteArray)).thenReturn(testBlock.toByteArray());

    blockchain = new Blockchain(mockBlockDB);

    assertEquals(testGenesisBlockHashByteArray, blockchain.getLastHash());
    assertEquals(testGenesisBlockHashByteArray, blockchain.getCurrentHash());
    Mockito.verify(mockBlockDB, Mockito.never()).putData(any(), eq(testGenesisBlockHashByteArray));

    byte[] blockBytes = blockchain.getBlockDB().getData(blockchain.getLastHash());
    assertEquals(testBlock, Block.parseFrom(blockBytes));

    logger.info("test blockchain: lastHash = {}, currentHash = {}",
        ByteArray.toHexString(blockchain.getLastHash()), ByteArray
            .toHexString(blockchain.getCurrentHash()));
  }

  @Test
  public void testFindTransaction() {
    Block testBlock = mockGenesisBlock(testGenesisBlockHash);
    Transaction testTransaction = testBlock.getTransactions(0);
    Mockito.when(mockBlockDB.getData(testGenesisBlockHashByteArray)).thenReturn(testBlock.toByteArray());

    Transaction result = blockchain.findTransaction(testTransaction.getId());

    assertEquals(testTransaction.getId(), result.getId());
    assertEquals(testTransaction.getVin(0).getTxID(), result.getVin(0).getTxID());
    assertEquals(testTransaction.getVout(0).getPubKeyHash(), result.getVout(0).getPubKeyHash());
    logger.info("{}", TransactionCapsule.toPrintString(result));
  }

  @Test
  public void testFindTransactionReturnsAnEmptyTransactionWhenTransactionIsNotFound() {
    Block testBlock = mockGenesisBlock(testGenesisBlockHash);
    Mockito.when(mockBlockDB.getData(testGenesisBlockHashByteArray)).thenReturn(testBlock.toByteArray());

    ByteString fakeTxId = ByteString.copyFrom(ByteArray.fromString(UUID.randomUUID().toString()));
    Transaction result = blockchain.findTransaction(fakeTxId);

    assertTrue(result.getSerializedSize() == 0);
    assertTrue(result.getVinCount() == 0);
    assertTrue(result.getVoutCount() == 0);
    logger.info("{}", TransactionCapsule.toPrintString(result));
  }

  @Test
  public void testFindUtxoFindsAllUnspentTransactionsFromBlockchainBlocks() {
    Block testGenesisBlock = mockGenesisBlock(testGenesisBlockHash);
    Mockito.when(mockBlockDB.getData(ByteArray.fromString(testGenesisBlockHash))).thenReturn(testGenesisBlock.toByteArray());

    String testLastBlockHash = "26g4099bb9e67fbc4bgdb45155cbe88gd71bddf61548b1b0e805b14b94d26d6e";
    Block testLastBlock = newTestBlock(testLastBlockHash, testGenesisBlockHash);
    Mockito.when(mockBlockDB.getData(ByteArray.fromString(testLastBlockHash))).thenReturn(testLastBlock.toByteArray());

    byte[] lastBlockHashByteArray = ByteArray.fromString(testLastBlockHash);
    blockchain.setLastHash(lastBlockHashByteArray);
    blockchain.setCurrentHash(lastBlockHashByteArray);
    HashMap<String, TXOutputs> utxo = blockchain.findUtxo();

    TXOutput lastBlockOutputs = utxo.get(ByteArray.toHexString(
        testLastBlock.getTransactions(0).getId().toByteArray())).getOutputs(0);
    assertEquals(testLastBlock.getTransactions(0).getVout(0), lastBlockOutputs);

    TXOutput genesisBlockOutputs = utxo.get(ByteArray.toHexString(
        testGenesisBlock.getTransactions(0).getId().toByteArray())).getOutputs(0);
    assertEquals(testGenesisBlock.getTransactions(0).getVout(0), genesisBlockOutputs);

    assertEquals(2, utxo.size());
    logger.info("{}", utxo);
  }

  @Test
  public void testFindUtxoDoesNotReturnSpentTransactions() {
    Block testGenesisBlock = mockGenesisBlock(testGenesisBlockHash);
    Mockito.when(mockBlockDB.getData(ByteArray.fromString(testGenesisBlockHash))).thenReturn(testGenesisBlock.toByteArray());

    String testLastBlockHash = "26g4099bb9e67fbc4bgdb45155cbe88gd71bddf61548b1b0e805b14b94d26d6e";
    Block testLastBlock = newTestBlock(testLastBlockHash, testGenesisBlockHash);
    testLastBlock = testLastBlock.toBuilder().addTransactions(
        testLastBlock.getTransactions(0).toBuilder()
            .clearVin().addVin(
        TXInput.getDefaultInstance().toBuilder()
            .setTxID(testGenesisBlock.getTransactions(0).getId())
            .setVout(0)
            .build()
        )
            .build()
    ).build();

    Mockito.when(mockBlockDB.getData(ByteArray.fromString(testLastBlockHash))).thenReturn(testLastBlock.toByteArray());

    byte[] lastBlockHashByteArray = ByteArray.fromString(testLastBlockHash);
    blockchain.setLastHash(lastBlockHashByteArray);
    blockchain.setCurrentHash(lastBlockHashByteArray);
    HashMap<String, TXOutputs> utxo = blockchain.findUtxo();

    TXOutput lastBlockOutputs = utxo.get(ByteArray.toHexString(
        testLastBlock.getTransactions(0).getId().toByteArray())).getOutputs(0);
    assertEquals(testLastBlock.getTransactions(0).getVout(0), lastBlockOutputs);

    TXOutputs genesisBlockOutputs = utxo.get(ByteArray.toHexString(
        testGenesisBlock.getTransactions(0).getId().toByteArray()));
    assertNull(genesisBlockOutputs);

    assertEquals(1, utxo.size());
    logger.info("{}", utxo);
  }

  @Test
  public void testAddBlockToChain() {
    String testNewBlockHash = "26g4099bb9e67fbc4bgdb45155cbe88gd71bddf61548b1b0e805b14b94d26d6e";
    Block testBlock = newTestBlock(testNewBlockHash, testGenesisBlockHash);

    blockchain.addBlock(testBlock);
  }

}
