import random
import json
import datetime
import uuid
import os
from typing import Dict, List
import argparse

class TransactionGenerator:
    def __init__(self):
        self.transaction_types = ['PURCHASE', 'REFUND', 'ADJUSTMENT', 'PAYMENT', 'TRANSFER']
        self.statuses = ['COMPLETED', 'PENDING', 'FAILED', 'CANCELLED']
        self.currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD']
        self.merchants = [
            'Etsy', 'Patreon', 'Skillshare', 'Masterclass', 'Udemy',
            'Khan Academy', 'Codecademy', 'Duolingo', 'Coursera', 'edX'
        ]

    def generate_transaction(self) -> Dict:
        """Generate a single transaction with realistic data."""
        amount = round(random.uniform(1.0, 1000.0), 2)
        transaction_type = random.choice(self.transaction_types)

        # Adjust amount range based on transaction type
        if transaction_type == 'REFUND':
            amount = -amount
        elif transaction_type == 'ADJUSTMENT':
            amount = round(random.uniform(-100.0, 100.0), 2)

        # Generate timestamp with microsecond precision
        timestamp = datetime.datetime.now() - datetime.timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
            microseconds=random.randint(0, 999999)
        )

        return {
            'transactionId': str(uuid.uuid4()),  # Use camelCase for consistency
            'timestamp': timestamp.isoformat(timespec='microseconds'),  # Format with microseconds
            'type': transaction_type,
            'amount': amount,
            'currency': random.choice(self.currencies),
            'status': random.choice(self.statuses),
            'merchant': random.choice(self.merchants),
            'customerId': str(uuid.uuid4()),  # Use camelCase for consistency
            'paymentMethod': random.choice(['CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER']),  # Use camelCase
            'metadata': {
                'location': f"{random.randint(-90, 90)},{random.randint(-180, 180)}",
                'deviceId': str(uuid.uuid4()),  # Use camelCase
                'ipAddress': f"{random.randint(1, 255)}.{random.randint(1, 255)}."
                             f"{random.randint(1, 255)}.{random.randint(1, 255)}"  # Use camelCase
            }
        }

    def generate_batch(self, count: int, output_dir: str, batch_size: int = 1000) -> None:
        """Generate transactions in batches to manage memory efficiently."""
        os.makedirs(output_dir, exist_ok=True)

        total_batches = (count + batch_size - 1) // batch_size
        current_count = 0

        print(f"Generating {count} transactions in {total_batches} batches...")

        for batch_num in range(total_batches):
            batch_transactions = []
            batch_size_actual = min(batch_size, count - current_count)

            for _ in range(batch_size_actual):
                batch_transactions.append(self.generate_transaction())
                current_count += 1

            # Write batch to file
            filename = os.path.join(output_dir, f'transactions_batch_{batch_num + 1}.json')
            with open(filename, 'w') as f:
                json.dump(batch_transactions, f, indent=2)

            progress = (current_count / count) * 100
            print(f"Progress: {progress:.1f}% - Generated batch {batch_num + 1}/{total_batches}")

        # Create a manifest file
        manifest = {
            'total_transactions': count,
            'total_batches': total_batches,
            'batch_size': batch_size,
            'generated_at': datetime.datetime.now().isoformat(),
            'files': [f'transactions_batch_{i + 1}.json' for i in range(total_batches)]
        }

        with open(os.path.join(output_dir, 'manifest.json'), 'w') as f:
            json.dump(manifest, f, indent=2)

        print(f"\nGeneration complete!")
        print(f"Total transactions: {count}")
        print(f"Total batches: {total_batches}")
        print(f"Output directory: {os.path.abspath(output_dir)}")

def main():
    parser = argparse.ArgumentParser(description='Generate test transaction data')
    parser.add_argument('--count', type=int, default=2000000,
                        help='Number of transactions to generate (default: 2,000,000)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of transactions per batch file (default: 1,000)')
    parser.add_argument('--output-dir', type=str, default='../data',
                        help='Output directory for generated files (default: ./data)')

    args = parser.parse_args()

    generator = TransactionGenerator()
    generator.generate_batch(args.count, args.output_dir, args.batch_size)

if __name__ == '__main__':
    main()