const { ethers } = require('ethers');

// 全域測試參數
const CONFIG = {
    NUM_USERS: 10000,  // 先用 1000 個用戶測試，之後再增加
    NUM_TRANSACTIONS: 1000,
    FUND_AMOUNT_ETH: 10,
    GAS_PRICE_GWEI: 20,
    GAS_LIMIT: 21000,
    MIN_TX_AMOUNT: 0.0001,
    MAX_TX_AMOUNT: 0.001,
    PROGRESS_LOG_INTERVAL: 50  // 每 N 筆交易輸出進度
};

console.log('🔧 測試配置:', CONFIG);

class GethLevelDBTest {
    constructor() {
        this.provider = new ethers.JsonRpcProvider('http://127.0.0.1:8545');
        this.users = [];
        this.userNonces = new Map();
        this.devAccount = null;
        this.successCount = 0;
        this.errorCount = 0;
    }

    async checkAndRecoverStuckBlocks() {
        const currentBlock = await this.provider.getBlockNumber();
        await new Promise(r => setTimeout(r, 2000)); // 等待 2 秒
        const newBlock = await this.provider.getBlockNumber();
        
        if (currentBlock === newBlock) {
            console.log(`⚠️  偵測到區塊停止在 ${currentBlock}，嘗試觸發挖礦...`);
            
            // 嘗試發送一筆簡單交易觸發挖礦
            try {
                const tx = await this.provider.send('eth_sendTransaction', [{
                    from: this.devAccount,
                    to: this.devAccount,
                    value: '0x1', // 1 wei
                    gas: ethers.toQuantity(21000)
                }]);
                console.log(`⚙️ 已發送觸發交易: ${tx}`);
                
                // 等待新區塊
                await new Promise(r => setTimeout(r, 3000));
                const finalBlock = await this.provider.getBlockNumber();
                console.log(`✅ 區塊從 ${currentBlock} 恢復到 ${finalBlock}`);
                
            } catch (error) {
                console.log(`❌ 無法觸發挖礦: ${error.message}`);
                throw new Error('區塊鏈停止，請手動重啟 Geth');
            }
        }
    }

    async initialize() {
        console.log('🚀 初始化測試環境...');

        // 檢查 Geth 連接
        try {
            const network = await this.provider.getNetwork();
            const blockNumber = await this.provider.getBlockNumber();
            console.log(`✅ 已連接到 Geth (ChainID: ${network.chainId}, 區塊: ${blockNumber})`);
        } catch (error) {
            console.error('❌ 無法連接到 Geth，請確認 Geth 正在運行於 http://127.0.0.1:8545');
            throw error;
        }

        // 獲取 dev account
        const accounts = await this.provider.send('eth_accounts', []);
        this.devAccount = accounts[0];
        console.log(`Dev account: ${this.devAccount}`);

        // 生成用戶
        console.log(`👥 生成 ${CONFIG.NUM_USERS} 個用戶...`);
        const progressStep = Math.max(1, Math.floor(CONFIG.NUM_USERS / 10));
        for (let i = 0; i < CONFIG.NUM_USERS; i++) {
            const wallet = ethers.Wallet.createRandom().connect(this.provider);
            this.users.push(wallet);
            this.userNonces.set(wallet.address, 0);

            if ((i + 1) % progressStep === 0) {
                console.log(`已生成 ${i + 1}/${CONFIG.NUM_USERS} 個用戶...`);
            }
        }
        console.log(`✅ 已生成 ${this.users.length} 個用戶`);
    }

    async fundUsers() {
        console.log('💰 為用戶分配資金...');
        const fundAmount = ethers.parseEther(CONFIG.FUND_AMOUNT_ETH.toString());
        const progressStep = Math.max(1, Math.floor(CONFIG.NUM_USERS / 10));

        for (let i = 0; i < this.users.length; i++) {
            const user = this.users[i];
            try {
                await this.provider.send('eth_sendTransaction', [{
                    from: this.devAccount,
                    to: user.address,
                    value: ethers.toQuantity(fundAmount),
                    gas: ethers.toQuantity(CONFIG.GAS_LIMIT)
                }]);

                if ((i + 1) % progressStep === 0) {
                    console.log(`已資助 ${i + 1}/${CONFIG.NUM_USERS} 用戶`);
                }

                if (i % 10 === 9) await new Promise(r => setTimeout(r, 500));

            } catch (error) {
                console.error(`資助用戶 ${i} 失敗:`, error.message);
            }
        }

        await new Promise(r => setTimeout(r, 3000));
        console.log('✅ 資金分配完成');
    }

    async sendUserTransaction(fromIndex, toIndex, amount) {
        const sender = this.users[fromIndex];
        const receiver = this.users[toIndex];
        const currentNonce = this.userNonces.get(sender.address);

        const tx = await sender.sendTransaction({
            to: receiver.address,
            value: ethers.parseEther(amount.toString()),
            gasLimit: CONFIG.GAS_LIMIT,
            gasPrice: ethers.parseUnits(CONFIG.GAS_PRICE_GWEI.toString(), 'gwei'),
            nonce: currentNonce
        });

        this.userNonces.set(sender.address, currentNonce + 1);

        // 等待交易確認
        const receipt = await Promise.race([
            tx.wait(1),
            new Promise((_, reject) =>
                setTimeout(() => reject(new Error('交易確認超時')), 60000)
            )
        ]);

        return { tx, receipt };
    }

    async runUserTransactions() {
        console.log(`🔥 執行 ${CONFIG.NUM_TRANSACTIONS} 筆用戶間交易...`);
        const startTime = Date.now();

        for (let i = 0; i < CONFIG.NUM_TRANSACTIONS; i++) {
            try {
                // 隨機選擇發送者和接收者
                const fromIndex = Math.floor(Math.random() * CONFIG.NUM_USERS);
                let toIndex = Math.floor(Math.random() * CONFIG.NUM_USERS);
                while (toIndex === fromIndex) {
                    toIndex = Math.floor(Math.random() * CONFIG.NUM_USERS);
                }

                const amount = (
                    Math.random() * (CONFIG.MAX_TX_AMOUNT - CONFIG.MIN_TX_AMOUNT) +
                    CONFIG.MIN_TX_AMOUNT
                ).toFixed(6);

                await this.sendUserTransaction(fromIndex, toIndex, amount);
                this.successCount++;

                if ((i + 1) % CONFIG.PROGRESS_LOG_INTERVAL === 0) {
                    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                    console.log(`進度: ${i + 1}/${CONFIG.NUM_TRANSACTIONS} (${elapsed}s, 成功: ${this.successCount}, 失敗: ${this.errorCount})`);
                }

            } catch (error) {
                this.errorCount++;
                console.error(`交易 ${i + 1} 失敗:`, error.message);

                // 如果是超時錯誤，檢查是否區塊停止
                if (error.message.includes('超時')) {
                    console.log('🔄 交易超時，檢查區塊狀態...');
                    await this.checkAndRecoverStuckBlocks();
                }
            }
        }

        const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`✅ 用戶交易完成！耗時: ${totalTime}s`);
    }


    async run() {
        try {
            const testStartTime = Date.now();
            await this.initialize();
            await this.fundUsers();
            await this.runUserTransactions();

            const totalTime = ((Date.now() - testStartTime) / 1000).toFixed(1);
            console.log('\n🎉 測試完成！');
            console.log(`總耗時: ${totalTime}s`);
            console.log(`成功交易: ${this.successCount}/${CONFIG.NUM_TRANSACTIONS}`);
            console.log(`失敗交易: ${this.errorCount}/${CONFIG.NUM_TRANSACTIONS}`);
            console.log(`\n📊 Trie 統計數據已寫入: core/trie_stats.csv`);

        } catch (error) {
            console.error('❌ 測試失敗:', error);
        }
    }
}

// 執行測試
new GethLevelDBTest().run().then(() => process.exit(0)).catch(e => {
    console.error('程式錯誤:', e);
    process.exit(1);
});
