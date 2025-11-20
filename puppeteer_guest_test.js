const puppeteer = require('puppeteer');

async function testCryptoChartAsGuest() {
  console.log('🚀 Starting Puppeteer test with guest mode...');
  
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--disable-gpu'
    ]
  });
  
  try {
    const page = await browser.newPage();
    
    // Set viewport for consistent testing
    await page.setViewport({ width: 1920, height: 1080 });
    
    console.log('📄 Navigating to the crypto chart application...');
    await page.goto('http://localhost:5000', { 
      waitUntil: 'networkidle0',
      timeout: 30000 
    });
    
    console.log('👤 Testing guest mode login...');
    
    // Wait for the email modal to appear
    await page.waitForSelector('#email-modal', { visible: true, timeout: 10000 });
    
    // Click the "Continue as Guest" button
    await page.click('#modal-guest-btn');
    
    // Wait for the modal to disappear (indicating successful login)
    await page.waitForFunction(
      () => document.getElementById('email-modal').style.display === 'none',
      { timeout: 10000 }
    );
    
    console.log('✅ Guest login successful!');
    
    // Wait for the chart to load
    await page.waitForSelector('#chart', { timeout: 15000 });
    
    console.log('📊 Chart loaded, waiting for data...');
    
    // Wait for data to load (look for data points > 0)
    await page.waitForFunction(
      () => {
        const dataPoints = document.getElementById('data-points');
        return dataPoints && parseInt(dataPoints.textContent) > 0;
      },
      { timeout: 20000 }
    );
    
    const dataPoints = await page.$eval('#data-points', el => el.textContent);
    console.log(`📈 Data loaded: ${dataPoints} data points`);
    
    // Test some interactive features
    console.log('🖱️ Testing interactive features...');
    
    // Test symbol change
    await page.select('#symbol-select', 'ETHUSDT');
    await page.waitForTimeout(2000); // Wait for data to update
    
    const currentSymbol = await page.$eval('#current-symbol', el => el.textContent);
    console.log(`🔄 Symbol changed to: ${currentSymbol}`);
    
    // Test interval change
    await page.select('#interval-select', '4h');
    await page.waitForTimeout(2000); // Wait for data to update
    
    const currentInterval = await page.$eval('#current-interval', el => el.textContent);
    console.log(`⏰ Interval changed to: ${currentInterval}`);
    
    // Test chart interaction (pan mode)
    await page.click('#pan-mode-btn');
    console.log('✋ Pan mode activated');
    
    // Take a screenshot for verification
    await page.screenshot({ 
      path: 'crypto_chart_guest_test.png',
      fullPage: true 
    });
    
    console.log('📸 Screenshot saved as crypto_chart_guest_test.png');
    
    // Check if user is shown as guest
    const userDisplay = await page.$eval('#user-email', el => el.textContent);
    console.log(`👤 User display: ${userDisplay}`);
    
    console.log('🎉 All tests passed! Guest mode working correctly.');
    
  } catch (error) {
    console.error('❌ Test failed:', error);
    throw error;
  } finally {
    await browser.close();
  }
}

// Run the test
if (require.main === module) {
  testCryptoChartAsGuest()
    .then(() => {
      console.log('✅ Test completed successfully');
      process.exit(0);
    })
    .catch((error) => {
      console.error('💥 Test failed:', error);
      process.exit(1);
    });
}

module.exports = { testCryptoChartAsGuest };