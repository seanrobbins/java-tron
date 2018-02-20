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
import static org.mockito.Matchers.any;
import static org.tron.core.Constant.LAST_HASH;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.utils.ByteArray;
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
  private final BlockHeader mockGenesisBlockHeader = BlockHeader.newBuilder()
      .setHash(getNewMockHash())
      .build();
  private Block mockGenesisBlock = Block.newBuilder()
      .setBlockHeader(mockGenesisBlockHeader)
      .build();
  private final byte[] mockGenesisBlockHashByteArray = mockGenesisBlock
      .getBlockHeader().getHash().toByteArray();

  /**
   * setup fo BlockchainTest.
   */
  @Before
  public void setup() throws IOException {
    mockBlockDB = Mockito.mock(LevelDbDataSourceImpl.class);
    Mockito.when(mockBlockDB.getData(LAST_HASH)).thenReturn(mockGenesisBlockHashByteArray);
    Mockito.when(mockBlockDB.getData(mockGenesisBlockHashByteArray))
        .thenReturn(mockGenesisBlock.toByteArray());

    blockchain = new Blockchain(mockBlockDB);
  }

  @Test
  public void testBlockchainConstructorCreatesNewGenesisBlockWhenBlockDBIsEmpty() {
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
  public void testBlockchainConstructorLaunchesWithHashOfLatestBlockAsLastHashWhenBlockDBNotEmpty()
      throws InvalidProtocolBufferException {
    blockchain = new Blockchain(mockBlockDB);

    assertEquals(mockGenesisBlockHashByteArray, blockchain.getLastHash());
    assertEquals(mockGenesisBlockHashByteArray, blockchain.getCurrentHash());
    Mockito.verify(mockBlockDB, Mockito.never()).putData(any(), any());

    byte[] blockBytes = blockchain.getBlockDB().getData(blockchain.getLastHash());
    assertEquals(mockGenesisBlock, Block.parseFrom(blockBytes));

    logger.info("test blockchain: lastHash = {}, currentHash = {}",
        ByteArray.toHexString(blockchain.getLastHash()), ByteArray
            .toHexString(blockchain.getCurrentHash()));
  }

  @Test
  public void testFindTransaction() {
    Block block = addTestBlockToBlockDBWithAnUnspentTransaction(mockGenesisBlock.getBlockHeader().getHash());

    blockchain.setLastHash(block.getBlockHeader().getHash().toByteArray());
    blockchain.setCurrentHash(block.getBlockHeader().getHash().toByteArray());
    Transaction result = blockchain.findTransaction(
        block.getTransactions(0).getId()
    );

    assertEquals(block.getTransactions(0), result);
    logger.info("{}", TransactionCapsule.toPrintString(result));
  }

  @Test
  public void testFindTransactionReturnsAnEmptyTransactionWhenTransactionIsNotFound() {
    ByteString fakeTxId = getNewMockHash();
    Transaction result = blockchain.findTransaction(fakeTxId);

    assertTrue(result.getSerializedSize() == 0);
    assertTrue(result.getVinCount() == 0);
    assertTrue(result.getVoutCount() == 0);
    logger.info("{}", TransactionCapsule.toPrintString(result));
  }

  @Test
  public void testFindUtxoFindsAllUnspentTransactionsFromBlocks() {
    Block block = addTestBlockToBlockDBWithAnUnspentTransaction(mockGenesisBlock.getBlockHeader().getHash());
    Block block2 = addTestBlockToBlockDBWithAnUnspentTransaction(block.getBlockHeader().getHash());

    blockchain.setLastHash(block2.getBlockHeader().getHash().toByteArray());
    blockchain.setCurrentHash(block2.getBlockHeader().getHash().toByteArray());
    HashMap<String, TXOutputs> utxo = blockchain.findUtxo();

    TXOutput firstBlockOutputs = utxo.get(ByteArray.toHexString(
        block.getTransactions(0).getId().toByteArray())).getOutputs(0);

    TXOutput lastBlockOutputs = utxo.get(ByteArray.toHexString(
        block2.getTransactions(0).getId().toByteArray())).getOutputs(0);

    assertEquals(block.getTransactions(0).getVout(0), firstBlockOutputs);
    assertEquals(block2.getTransactions(0).getVout(0), lastBlockOutputs);
    assertEquals(2, utxo.size());

    logger.info("{}", utxo);
  }

  @Test
  public void testFindUtxoDoesNotReturnSpentTransactions() {
    Block block = addTestBlockToBlockDBWithAnUnspentTransaction(mockGenesisBlock.getBlockHeader().getHash());
    Block block2 = addTestBlockToBlockDBWithASpentTransactionFromParentBlock(block);
    Block block3 = addTestBlockToBlockDBWithAnUnspentTransaction(block2.getBlockHeader().getHash());

    blockchain.setLastHash(block3.getBlockHeader().getHash().toByteArray());
    blockchain.setCurrentHash(block3.getBlockHeader().getHash().toByteArray());
    HashMap<String, TXOutputs> utxo = blockchain.findUtxo();

    assertEquals(1, utxo.size());
    TXOutput blockOutputs = utxo.get(ByteArray.toHexString(
        block3.getTransactions(0).getId().toByteArray())).getOutputs(0);
    assertEquals(block3.getTransactions(0).getVout(0), blockOutputs);
    logger.info("{}", utxo);
  }

  @Test
  public void testAddBlockToChain() {
    Block block = Block.newBuilder().setBlockHeader(
        BlockHeader.newBuilder()
            .setHash(getNewMockHash())
            .setParentHash(mockGenesisBlock.getBlockHeader().getHash())
    ).build();

    blockchain.addBlock(block);
    Mockito.verify(mockBlockDB).getData(block.getBlockHeader().getHash().toByteArray());
    Mockito.verify(mockBlockDB, Mockito.never()).putData(any(), any());
  }

  private ByteString getNewMockHash() {
    return ByteString.copyFrom(RandomUtils.nextBytes(64));
  }

  private Block addTestBlockToBlockDBWithAnUnspentTransaction(ByteString parentBlockHash) {
    BlockHeader blockHeader = BlockHeader.newBuilder()
        .setHash(getNewMockHash())
        .setParentHash(parentBlockHash)
        .build();

    TXOutput output = TXOutput.newBuilder().setPubKeyHash(getNewMockHash())
        .setValue(RandomUtils.nextLong(0, 1000)).build();
    Transaction transaction = Transaction.newBuilder().setId(getNewMockHash()).addVout(output).build();

    Block block = Block.newBuilder()
        .setBlockHeader(blockHeader)
        .addTransactions(transaction)
        .build();

    Mockito.when(mockBlockDB.getData(block.getBlockHeader().getHash().toByteArray()))
        .thenReturn(block.toByteArray());

    return block;
  }

  private Block addTestBlockToBlockDBWithASpentTransactionFromParentBlock(Block parentBlock) {
    BlockHeader blockHeader = BlockHeader.newBuilder()
        .setHash(getNewMockHash())
        .setParentHash(parentBlock.getBlockHeader().getHash())
        .build();

    Block block = Block.newBuilder().setBlockHeader(blockHeader).build();

    TXInput input = TXInput.newBuilder()
        .setTxID(parentBlock.getTransactions(0).getId())
        .setVout(0)
        .build();

    Transaction transaction = Transaction.newBuilder().addVin(input).build();
    Mockito.when(mockBlockDB.getData(block.getBlockHeader().getHash().toByteArray()))
        .thenReturn(block.toBuilder().addTransactions(transaction).build().toByteArray());

    return block;
  }
}
