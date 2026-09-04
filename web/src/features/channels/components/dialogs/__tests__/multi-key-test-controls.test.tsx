/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, test, vi } from 'vitest'

import { MultiKeyTestControls } from '../multi-key-manage-dialog'

describe('MultiKeyTestControls', () => {
  test('shows the shared endpoint options and reports a selected endpoint', async () => {
    const onEndpointTypeChange = vi.fn()
    const user = userEvent.setup()

    render(
      <MultiKeyTestControls
        testModel='gpt-test'
        endpointType='auto'
        onTestModelChange={vi.fn()}
        onEndpointTypeChange={onEndpointTypeChange}
      />
    )

    const endpointTrigger = screen.getByRole('combobox', {
      name: 'Endpoint Type',
    })
    expect(endpointTrigger).toHaveTextContent('Auto detect (default)')

    await user.click(endpointTrigger)
    const responseOption = await screen.findByText(
      'OpenAI Responses (/v1/responses)'
    )
    await user.click(
      responseOption.closest('[data-slot="select-item"]') ?? responseOption
    )

    await waitFor(() => {
      expect(onEndpointTypeChange).toHaveBeenCalledWith('openai-response')
    })
  })

  test('reports edits to the temporary test model', () => {
    const onTestModelChange = vi.fn()

    render(
      <MultiKeyTestControls
        testModel='gpt-test'
        endpointType='auto'
        onTestModelChange={onTestModelChange}
        onEndpointTypeChange={vi.fn()}
      />
    )

    fireEvent.change(screen.getByRole('textbox', { name: 'Test model' }), {
      target: { value: 'claude-test' },
    })

    expect(onTestModelChange).toHaveBeenCalledWith('claude-test')
  })
})
